//go:build ignore

// coverage-gate runs Go tests with native statement coverage and with an
// AST-instrumented overlay that gives every supported branch outcome its own
// Go coverage counter. It deliberately compares raw integer counts: rounded
// percentages are never used to decide whether the gate passes.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const schema = "cobaltdb-coverage/v1"

var metricNames = [...]string{"statements", "functions", "lines", "branches"}

type metric struct {
	Covered int `json:"covered"`
	Total   int `json:"total"`
}

type report struct {
	Schema  string            `json:"schema"`
	Metrics map[string]metric `json:"metrics"`
}

type listedPackage struct {
	Dir        string
	ImportPath string
	GoFiles    []string
	CgoFiles   []string
}

type profileBlock struct {
	File                  string
	StartLine, StartCol   int
	EndLine, EndCol       int
	Statements, Execution int
}

type sourceFile struct {
	Path       string
	ProfileKey string
	AST        *ast.File
	Fset       *token.FileSet
}

type probeMarker struct {
	ProfileKey string
	Line       int
	Label      string
	Kind       string
}

type instrumenter struct {
	next      int
	generated map[ast.Node]bool
	probes    []ast.Decl
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: go run ./scripts/coverage-gate.go <run|check|self-test> [options]")
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCommand(os.Args[2:])
	case "check":
		err = checkCommand(os.Args[2:])
	case "self-test":
		err = selfTest()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(formatString string, args ...any) {
	fmt.Fprintf(os.Stderr, "coverage gate: "+formatString+"\n", args...)
	os.Exit(1)
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	root := fs.String("root", ".", "module root")
	out := fs.String("out", "coverage.json", "raw JSON report")
	profile := fs.String("profile", "coverage.out", "native Go cover profile")
	html := fs.String("html", "coverage.html", "native HTML report (empty disables it)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return runCoverage(*root, *out, *profile, *html, true)
}

func checkCommand(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("check requires exactly one JSON report path")
	}
	r, err := readReport(fs.Arg(0))
	if err != nil {
		return err
	}
	return enforce(r, os.Stdout)
}

func runCoverage(root, outPath, profilePath, htmlPath string, announce bool) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	packages, err := listPackages(absRoot)
	if err != nil {
		return err
	}
	if len(packages) == 0 {
		return errors.New("go list ./... found no packages")
	}

	tmp, err := os.MkdirTemp("", "cobaltdb-branch-coverage-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	branchRoot := filepath.Join(tmp, "module")
	if err := copyModule(absRoot, branchRoot); err != nil {
		return err
	}
	branchPackages, err := listPackages(branchRoot)
	if err != nil {
		return err
	}
	markers, err := instrumentSnapshot(branchPackages)
	if err != nil {
		return err
	}
	if len(markers) == 0 {
		return errors.New("branch instrumentation found no branch outcomes")
	}
	branchProfile := filepath.Join(tmp, "branches.out")
	if announce {
		fmt.Println("Running AST-instrumented branch coverage tests...")
	}
	if err := goCommand(branchRoot, "test", "-count=1", "-covermode=count", "-coverprofile="+branchProfile, "./..."); err != nil {
		return fmt.Errorf("branch-instrumented tests: %w", err)
	}

	absProfile := absoluteFrom(absRoot, profilePath)
	if announce {
		fmt.Println("Running native Go statement coverage tests...")
	}
	if err := goCommand(absRoot, "test", "-count=1", "-covermode=count", "-coverprofile="+absProfile, "./..."); err != nil {
		return fmt.Errorf("native coverage tests: %w", err)
	}

	nativeBlocks, err := parseProfile(absProfile)
	if err != nil {
		return err
	}
	branchBlocks, err := parseProfile(branchProfile)
	if err != nil {
		return err
	}
	sources, err := parseSources(packages)
	if err != nil {
		return err
	}
	r := report{Schema: schema, Metrics: map[string]metric{}}
	r.Metrics["statements"] = statementMetric(nativeBlocks)
	r.Metrics["functions"] = probeMetric(markers, branchBlocks, "function")
	r.Metrics["lines"] = lineMetric(sources, nativeBlocks)
	r.Metrics["branches"] = probeMetric(markers, branchBlocks, "branch")

	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(absoluteFrom(absRoot, outPath), encoded, 0o644); err != nil {
		return err
	}
	if htmlPath != "" {
		if err := goCommand(absRoot, "tool", "cover", "-html="+absProfile, "-o", absoluteFrom(absRoot, htmlPath)); err != nil {
			return fmt.Errorf("HTML coverage report: %w", err)
		}
	}
	return enforce(r, os.Stdout)
}

func absoluteFrom(root, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

func goCommand(root string, args ...string) error {
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func listPackages(root string) ([]listedPackage, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list ./...: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(out))
	var packages []listedPackage
	for {
		var p listedPackage
		if err := dec.Decode(&p); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode go list output: %w", err)
		}
		packages = append(packages, p)
	}
	return packages, nil
}

func copyModule(root, destination string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "bin" || entry.Name() == "dist") {
			return filepath.SkipDir
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
}

func instrumentSnapshot(packages []listedPackage) ([]probeMarker, error) {
	var markers []probeMarker
	inst := &instrumenter{generated: make(map[ast.Node]bool)}
	for _, p := range packages {
		files := append(append([]string(nil), p.GoFiles...), p.CgoFiles...)
		for _, name := range files {
			path := filepath.Join(p.Dir, name)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				return nil, fmt.Errorf("parse %s: %w", path, err)
			}
			inst.probes = nil
			ast.Walk(inst, file)
			inst.addRangeExitProbes(file)
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				fn.Body.List = append([]ast.Stmt{inst.marker("function")}, fn.Body.List...)
			}
			file.Decls = append(file.Decls, inst.probes...)
			var formatted bytes.Buffer
			if err := format.Node(&formatted, fset, file); err != nil {
				return nil, fmt.Errorf("format instrumented %s: %w", path, err)
			}
			if err := os.WriteFile(path, formatted.Bytes(), 0o600); err != nil {
				return nil, err
			}
			profileKey := pathKey(p.ImportPath, name)
			for lineNo, line := range strings.Split(formatted.String(), "\n") {
				const prefix = "func __cobaltdb_branch_"
				trimmed := strings.TrimSpace(line)
				if !strings.HasPrefix(trimmed, prefix) {
					continue
				}
				end := strings.IndexByte(trimmed, '(')
				if end < len("func ") {
					return nil, fmt.Errorf("invalid generated branch probe in %s", path)
				}
				label := trimmed[len("func "):end]
				kind := "branch"
				if strings.HasSuffix(label, "_function") {
					kind = "function"
				}
				markers = append(markers, probeMarker{ProfileKey: profileKey, Line: lineNo + 2, Label: label, Kind: kind})
			}
		}
	}
	return markers, nil
}

func pathKey(importPath, name string) string {
	return filepath.ToSlash(importPath + "/" + name)
}

func (v *instrumenter) Visit(node ast.Node) ast.Visitor {
	if node == nil || v.generated[node] {
		return nil
	}
	switch n := node.(type) {
	case *ast.IfStmt:
		n.Body.List = append([]ast.Stmt{v.marker("if-true")}, n.Body.List...)
		if n.Else == nil {
			block := &ast.BlockStmt{List: []ast.Stmt{v.marker("if-false")}}
			v.generated[block] = true
			n.Else = block
		} else if block, ok := n.Else.(*ast.BlockStmt); ok {
			block.List = append([]ast.Stmt{v.marker("if-false")}, block.List...)
		} else {
			// Do not mark this wrapper generated: walking through it is how a
			// nested else-if receives its own true/false probes.
			block := &ast.BlockStmt{List: []ast.Stmt{v.marker("if-false"), n.Else.(ast.Stmt)}}
			n.Else = block
		}
	case *ast.ForStmt:
		if n.Cond != nil {
			trueBlock := &ast.BlockStmt{List: []ast.Stmt{v.marker("for-true")}}
			falseBlock := &ast.BlockStmt{List: []ast.Stmt{v.marker("for-false"), &ast.BranchStmt{Tok: token.BREAK}}}
			check := &ast.IfStmt{Cond: n.Cond, Body: trueBlock, Else: falseBlock}
			v.generated[trueBlock] = true
			v.generated[falseBlock] = true
			v.generated[check] = true
			n.Cond = nil
			n.Body.List = append([]ast.Stmt{check}, n.Body.List...)
		}
	case *ast.RangeStmt:
		n.Body.List = append([]ast.Stmt{v.marker("range-body")}, n.Body.List...)
	case *ast.SwitchStmt:
		v.instrumentClauses(n.Body, "switch")
	case *ast.TypeSwitchStmt:
		v.instrumentClauses(n.Body, "type-switch")
	case *ast.SelectStmt:
		v.instrumentClauses(n.Body, "select")
	}
	return v
}

func (v *instrumenter) addRangeExitProbes(file *ast.File) {
	ast.Inspect(file, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok || v.generated[block] {
			return true
		}
		for index := 0; index < len(block.List); index++ {
			if _, ok := block.List[index].(*ast.RangeStmt); !ok {
				continue
			}
			block.List = append(block.List, nil)
			copy(block.List[index+2:], block.List[index+1:])
			block.List[index+1] = v.marker("range-exit")
			index++
		}
		return true
	})
}

func (v *instrumenter) marker(kind string) ast.Stmt {
	v.next++
	name := fmt.Sprintf("__cobaltdb_branch_%08d_%s", v.next, strings.ReplaceAll(kind, "-", "_"))
	probeBody := &ast.BlockStmt{List: []ast.Stmt{&ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent("_")}, Tok: token.ASSIGN, Rhs: []ast.Expr{&ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(name)}}}}}
	probe := &ast.FuncDecl{Name: ast.NewIdent(name), Type: &ast.FuncType{Params: &ast.FieldList{}}, Body: probeBody}
	v.generated[probe] = true
	v.probes = append(v.probes, probe)
	stmt := &ast.ExprStmt{X: &ast.CallExpr{Fun: ast.NewIdent(name)}}
	v.generated[stmt] = true
	return stmt
}

func (v *instrumenter) instrumentClauses(body *ast.BlockStmt, kind string) {
	hasDefault := false
	for _, item := range body.List {
		switch clause := item.(type) {
		case *ast.CaseClause:
			if clause.List == nil {
				hasDefault = true
			}
			clause.Body = append([]ast.Stmt{v.marker(kind + "-case")}, clause.Body...)
		case *ast.CommClause:
			if clause.Comm == nil {
				hasDefault = true
			}
			clause.Body = append([]ast.Stmt{v.marker(kind + "-case")}, clause.Body...)
		}
	}
	// A no-match outcome is a real branch for switches. A select cannot gain a
	// default: doing so would change its blocking behavior.
	if kind != "select" && !hasDefault {
		clause := &ast.CaseClause{Body: []ast.Stmt{v.marker(kind + "-default")}}
		v.generated[clause] = true
		body.List = append(body.List, clause)
	}
}

func parseProfile(path string) ([]profileBlock, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "mode:") {
		return nil, fmt.Errorf("invalid Go cover profile %s", path)
	}
	blocks := make([]profileBlock, 0, len(lines)-1)
	for _, line := range lines[1:] {
		lastColon := strings.LastIndex(line, ":")
		if lastColon < 0 {
			return nil, fmt.Errorf("invalid cover profile row %q", line)
		}
		var b profileBlock
		var start, end string
		if _, err := fmt.Sscanf(line[lastColon+1:], "%s %d %d", &start, &b.Statements, &b.Execution); err != nil {
			return nil, fmt.Errorf("invalid cover profile row %q: %w", line, err)
		}
		b.File = filepath.ToSlash(line[:lastColon])
		parts := strings.SplitN(start, ",", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid cover range %q", start)
		}
		if _, err := fmt.Sscanf(parts[0], "%d.%d", &b.StartLine, &b.StartCol); err != nil {
			return nil, err
		}
		end = parts[1]
		if _, err := fmt.Sscanf(end, "%d.%d", &b.EndLine, &b.EndCol); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

func parseSources(packages []listedPackage) ([]sourceFile, error) {
	var sources []sourceFile
	for _, p := range packages {
		for _, name := range append(append([]string(nil), p.GoFiles...), p.CgoFiles...) {
			path := filepath.Join(p.Dir, name)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return nil, err
			}
			sources = append(sources, sourceFile{Path: path, ProfileKey: pathKey(p.ImportPath, name), AST: file, Fset: fset})
		}
	}
	return sources, nil
}

func statementMetric(blocks []profileBlock) metric {
	var result metric
	for _, b := range blocks {
		result.Total += b.Statements
		if b.Execution > 0 {
			result.Covered += b.Statements
		}
	}
	return result
}

func matchingBlocks(key string, blocks []profileBlock) []profileBlock {
	var result []profileBlock
	for _, b := range blocks {
		if b.File == key || strings.HasSuffix(b.File, "/"+key) || strings.HasSuffix(key, "/"+b.File) {
			result = append(result, b)
		}
	}
	return result
}

func containsPosition(b profileBlock, pos token.Position) bool {
	if pos.Line < b.StartLine || pos.Line > b.EndLine {
		return false
	}
	if pos.Line == b.StartLine && pos.Column < b.StartCol {
		return false
	}
	if pos.Line == b.EndLine && pos.Column >= b.EndCol {
		return false
	}
	return true
}

func lineMetric(sources []sourceFile, blocks []profileBlock) metric {
	var result metric
	for _, source := range sources {
		fileBlocks := matchingBlocks(source.ProfileKey, blocks)
		lineCovered := make(map[int]bool)
		ast.Inspect(source.AST, func(node ast.Node) bool {
			stmt, ok := node.(ast.Stmt)
			if !ok {
				return true
			}
			switch stmt.(type) {
			case *ast.BlockStmt, *ast.EmptyStmt:
				return true
			}
			pos := source.Fset.Position(stmt.Pos())
			for _, b := range fileBlocks {
				if containsPosition(b, pos) {
					lineCovered[pos.Line] = lineCovered[pos.Line] || b.Execution > 0
					break
				}
			}
			return true
		})
		result.Total += len(lineCovered)
		for _, covered := range lineCovered {
			if covered {
				result.Covered++
			}
		}
	}
	return result
}

func probeMetric(markers []probeMarker, blocks []profileBlock, kind string) metric {
	var result metric
	for _, marker := range markers {
		if marker.Kind != kind {
			continue
		}
		result.Total++
		for _, b := range matchingBlocks(marker.ProfileKey, blocks) {
			if marker.Line >= b.StartLine && marker.Line <= b.EndLine && b.Execution > 0 {
				result.Covered++
				break
			}
		}
	}
	return result
}

func readReport(path string) (report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return report{}, err
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return report{}, fmt.Errorf("invalid report: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var r report
	if err := dec.Decode(&r); err != nil {
		return report{}, fmt.Errorf("invalid report: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return report{}, errors.New("invalid report: trailing JSON value")
	}
	return r, nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	var consume func() error
	consume = func() error {
		tokenValue, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := tokenValue.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for dec.More() {
				keyToken, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				if seen[key] {
					return fmt.Errorf("duplicate JSON key %q", key)
				}
				seen[key] = true
				if err := consume(); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		case '[':
			for dec.More() {
				if err := consume(); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delim)
		}
	}
	return consume()
}

func validate(r report) error {
	if r.Schema != schema {
		return fmt.Errorf("unsupported schema %q (want %q)", r.Schema, schema)
	}
	if len(r.Metrics) != len(metricNames) {
		return fmt.Errorf("metrics must contain exactly %s", strings.Join(metricNames[:], ", "))
	}
	for _, name := range metricNames {
		m, ok := r.Metrics[name]
		if !ok {
			return fmt.Errorf("missing %s metric", name)
		}
		if m.Total <= 0 {
			return fmt.Errorf("%s total must be a positive raw integer (got %d)", name, m.Total)
		}
		if m.Covered < 0 || m.Covered > m.Total {
			return fmt.Errorf("invalid %s raw counts: %d/%d", name, m.Covered, m.Total)
		}
	}
	return nil
}

func enforce(r report, out io.Writer) error {
	if err := validate(r); err != nil {
		return err
	}
	failed := false
	for _, name := range metricNames {
		m := r.Metrics[name]
		status := "PASS"
		if m.Covered != m.Total {
			status = "FAIL"
			failed = true
		}
		fmt.Fprintf(out, "%-10s %d/%d %s\n", name+":", m.Covered, m.Total, status)
	}
	if failed {
		return errors.New("raw coverage counts are below 100%; every covered count must equal its total")
	}
	fmt.Fprintln(out, "coverage gate: all four raw metrics are exactly 100%")
	return nil
}

func selfTest() error {
	// First test the fail-closed report contract, independently for each metric.
	perfect := report{Schema: schema, Metrics: map[string]metric{}}
	for _, name := range metricNames {
		perfect.Metrics[name] = metric{Covered: 10, Total: 10}
	}
	if err := enforce(perfect, io.Discard); err != nil {
		return fmt.Errorf("perfect report rejected: %w", err)
	}
	for _, missing := range metricNames {
		candidate := cloneReport(perfect)
		candidate.Metrics[missing] = metric{Covered: 9, Total: 10}
		if err := enforce(candidate, io.Discard); err == nil {
			return fmt.Errorf("uncovered %s was accepted", missing)
		}
	}
	for name, mutate := range map[string]func(*report){
		"missing branch":     func(r *report) { delete(r.Metrics, "branches") },
		"zero total":         func(r *report) { r.Metrics["lines"] = metric{} },
		"covered over total": func(r *report) { r.Metrics["functions"] = metric{Covered: 11, Total: 10} },
		"rounds to 100":      func(r *report) { r.Metrics["statements"] = metric{Covered: 99999, Total: 100000} },
		"unknown metric":     func(r *report) { r.Metrics["conditions"] = metric{Covered: 1, Total: 1} },
		"unknown schema":     func(r *report) { r.Schema = "other/v1" },
	} {
		candidate := cloneReport(perfect)
		mutate(&candidate)
		if err := enforce(candidate, io.Discard); err == nil {
			return fmt.Errorf("invalid fixture %q was accepted", name)
		}
	}
	for name, contents := range map[string]string{
		"malformed JSON":   `{`,
		"fractional count": `{"schema":"cobaltdb-coverage/v1","metrics":{"statements":{"covered":1.5,"total":2}}}`,
		"unknown field":    `{"schema":"cobaltdb-coverage/v1","metrics":{},"percent":100}`,
		"duplicate metric": `{"schema":"cobaltdb-coverage/v1","metrics":{"branches":{"covered":1,"total":1},"branches":{"covered":0,"total":1}}}`,
		"trailing value":   `{"schema":"cobaltdb-coverage/v1","metrics":{}} {}`,
	} {
		path := filepath.Join(os.TempDir(), fmt.Sprintf("coverage-gate-invalid-%d.json", os.Getpid()))
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			return err
		}
		_, err := readReport(path)
		_ = os.Remove(path)
		if err == nil {
			return fmt.Errorf("invalid JSON fixture %q was accepted", name)
		}
	}

	// Then prove the AST counters distinguish a genuinely uncovered branch from
	// 100% native coverage. The false arm contains no source statement, so only
	// real branch instrumentation can see that the one-sided test missed it.
	root, err := os.MkdirTemp("", "coverage-gate-self-test-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.test/fixture\n\ngo 1.25\n"), 0o600); err != nil {
		return err
	}
	source := "package fixture\n\nfunc bump(v bool) int {\n\tn := 0\n\tif v { n++ }\n\treturn n\n}\n"
	if err := os.WriteFile(filepath.Join(root, "fixture.go"), []byte(source), 0o600); err != nil {
		return err
	}
	oneSided := "package fixture\n\nimport \"testing\"\n\nfunc TestBump(t *testing.T) { if bump(true) != 1 { t.Fatal() } }\n"
	if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte(oneSided), 0o600); err != nil {
		return err
	}
	oneReport := filepath.Join(root, "one.json")
	if err := runCoverage(root, oneReport, filepath.Join(root, "one.out"), "", false); err == nil || !strings.Contains(err.Error(), "below 100%") {
		return fmt.Errorf("one-sided branch fixture did not fail specifically at the gate: %v", err)
	}
	r, err := readReport(oneReport)
	if err != nil {
		return err
	}
	if r.Metrics["statements"].Covered != r.Metrics["statements"].Total || r.Metrics["branches"].Covered == r.Metrics["branches"].Total {
		return fmt.Errorf("instrumentation fixture did not isolate branch coverage: %+v", r.Metrics)
	}
	twoSided := "package fixture\n\nimport \"testing\"\n\nfunc TestBump(t *testing.T) {\n\tif bump(true) != 1 { t.Fatal() }\n\tif bump(false) != 0 { t.Fatal() }\n}\n"
	if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte(twoSided), 0o600); err != nil {
		return err
	}
	if err := runCoverage(root, filepath.Join(root, "two.json"), filepath.Join(root, "two.out"), "", false); err != nil {
		return fmt.Errorf("two-sided instrumentation fixture: %w", err)
	}
	fmt.Println("coverage gate self-tests: PASS")
	return nil
}

func cloneReport(input report) report {
	result := report{Schema: input.Schema, Metrics: make(map[string]metric, len(input.Metrics))}
	keys := make([]string, 0, len(input.Metrics))
	for key := range input.Metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Metrics[key] = input.Metrics[key]
	}
	return result
}

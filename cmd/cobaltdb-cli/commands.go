package main

import (
	"fmt"
	"os"
)

// Command represents a CLI subcommand.
type Command interface {
	// Name is the subcommand name (e.g. "backup", "import").
	Name() string
	// Run executes the command with the given args and DB connection parameters.
	Run(args []string, path string, inMemory bool)
}

// commandRegistry maps subcommand names to their implementations.
var commandRegistry = make(map[string]Command)

// RegisterCommand adds a command to the registry.
func RegisterCommand(cmd Command) {
	commandRegistry[cmd.Name()] = cmd
}

// RunCommand looks up a subcommand by name and runs it.
// Returns true if the name was a registered subcommand, false otherwise.
func RunCommand(name string, args []string, path string, inMemory bool) bool {
	cmd, ok := commandRegistry[name]
	if !ok {
		return false
	}
	cmd.Run(args, path, inMemory)
	return true
}

// --- concrete commands ---

type backupCommand struct{}

func (c *backupCommand) Name() string { return "backup" }
func (c *backupCommand) Run(args []string, path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()
	if len(args) == 0 {
		fmt.Println("Usage: backup create [full|incremental|differential]")
		fmt.Println("       backup list")
		fmt.Println("       backup restore <id>")
		fmt.Println("       backup delete <id>")
		closeDBAndExit(db, 1)
	}
	handleBackupCommand(args, db)
}

type metricsCommand struct{}

func (c *metricsCommand) Name() string { return "metrics" }
func (c *metricsCommand) Run(_ []string, path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()
	data, err := db.GetMetrics()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		closeDBAndExit(db, 1)
	}
	fmt.Println(string(data))
}

type statusCommand struct{}

func (c *statusCommand) Name() string { return "status" }
func (c *statusCommand) Run(_ []string, path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()
	printStatus(db)
}

type vacuumCommand struct{}

func (c *vacuumCommand) Name() string { return "vacuum" }
func (c *vacuumCommand) Run(_ []string, path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()
	executeSQL(db, "VACUUM")
}

type analyzeCommand struct{}

func (c *analyzeCommand) Name() string { return "analyze" }
func (c *analyzeCommand) Run(_ []string, path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()
	executeSQL(db, "ANALYZE")
}

type importCommand struct{}

func (c *importCommand) Name() string { return "import" }
func (c *importCommand) Run(args []string, path string, inMemory bool) {
	if len(args) < 2 {
		fmt.Println("Usage: import <file.csv> <table>")
		os.Exit(1)
	}
	db := openDB(path, inMemory)
	defer db.Close()
	if err := importCSV(db, args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		closeDBAndExit(db, 1)
	}
}

type exportCommand struct{}

func (c *exportCommand) Name() string { return "export" }
func (c *exportCommand) Run(args []string, path string, inMemory bool) {
	runExportCommand(args, path, inMemory)
}

type dumpCommand struct{}

func (c *dumpCommand) Name() string { return "dump" }
func (c *dumpCommand) Run(args []string, path string, inMemory bool) {
	runDumpCommand(args, path, inMemory)
}

type restoreCommand struct{}

func (c *restoreCommand) Name() string { return "restore" }
func (c *restoreCommand) Run(args []string, path string, inMemory bool) {
	runRestoreCommand(args, path, inMemory)
}

// init registers all built-in commands.
func init() {
	RegisterCommand(&backupCommand{})
	RegisterCommand(&metricsCommand{})
	RegisterCommand(&statusCommand{})
	RegisterCommand(&vacuumCommand{})
	RegisterCommand(&analyzeCommand{})
	RegisterCommand(&importCommand{})
	RegisterCommand(&exportCommand{})
	RegisterCommand(&dumpCommand{})
	RegisterCommand(&restoreCommand{})
}

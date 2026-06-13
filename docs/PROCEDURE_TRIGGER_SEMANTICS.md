# Procedure And Trigger Semantics

This page records the currently certified stored procedure and trigger behavior.

## Stored Procedures

Certified behavior:

- `CREATE PROCEDURE`, `CREATE PROCEDURE IF NOT EXISTS`, `DROP PROCEDURE`, and `DROP PROCEDURE IF EXISTS`.
- `CALL proc(...)` with SQL literal arguments.
- `CALL proc(?, ?)` with API-supplied placeholder arguments.
- Exact argument count validation. Missing and extra arguments fail before the procedure body runs.
- Parameter substitution in `INSERT`, `UPDATE`, and `DELETE` procedure body statements.
- Parameter substitution inside common expression forms including binary expressions, function calls, `CASE`, `BETWEEN`, `IN`, `IS NULL`, `CAST`, `LIKE`, and aliases.
- Multi-statement procedure bodies.
- `OUT` and `INOUT` parameter modes are preserved by parsing/persistence.
- `SET out_param = expr` and `SET inout_param = expr` inside a procedure body update output parameter values.
- `Query("CALL proc(...)")` returns one row containing `OUT`/`INOUT` values, in procedure parameter order.
- `Query("CALL proc(...)")` returns the final result-producing statement when a procedure body contains `SELECT`/SHOW-like result statements; DML side effects before the final result are preserved.

Current limits:

- `Exec("CALL proc(...)")` is certified for DML and output-parameter assignment procedures. Use `Query("CALL proc(...)")` for result-returning procedure bodies.
- Output parameter assignment is certified for scalar expressions that can be parsed through the current expression parser; broader MySQL session-variable binding semantics are not implemented.
- Multiple result sets from one procedure call are not implemented; if a procedure body contains multiple result-producing statements, `Query("CALL ...")` returns the last one.

## Triggers

Certified behavior:

- Row-level `BEFORE` and `AFTER` triggers for `INSERT`, `UPDATE`, and `DELETE`.
- `INSTEAD OF` triggers for view `INSERT`, `UPDATE`, and `DELETE`.
- `NEW.column` and `OLD.column` row image resolution.
- `WHEN` condition evaluation for trigger filtering.
- Multi-statement trigger bodies.

Current limits:

- Trigger bodies are certified for `INSERT`, `UPDATE`, and `DELETE` statements.
- Trigger side effects execute through the current catalog execution path; recursive trigger behavior should be workload-tested before production use.

## Release Drill

```bash
go test ./pkg/query -run 'TestParseCallProcedure|TestParseCreateProcedureParamModes' -count=1
go test ./test -run 'TestStoredProcedure|TestTrigger_BeforeAfterOrderAndRowImages|TestInsteadOfTrigger' -count=1
go test ./pkg/catalog -run 'TestExecuteTriggers|TestResolveTriggerRefs|TestResolveTriggerExpr' -count=1
```

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

Current limits:

- Procedure body execution is certified for DML statements. Result-returning procedure semantics are not a compatibility contract.
- `OUT` and `INOUT` parameters parse, but mutable output-parameter behavior is not certified.

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
go test ./pkg/query -run TestParseCallProcedure -count=1
go test ./test -run 'TestStoredProcedure|TestTrigger_BeforeAfterOrderAndRowImages|TestInsteadOfTrigger' -count=1
go test ./pkg/catalog -run 'TestExecuteTriggers|TestResolveTriggerRefs|TestResolveTriggerExpr' -count=1
```

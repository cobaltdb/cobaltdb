# Changelog

All notable changes to CobaltDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.1.0] - 2026-03-01

### Added
- **WAL (Write-Ahead Log)**: Complete crash recovery support
  - Logs all INSERT, UPDATE, DELETE operations before applying
  - Transaction support with COMMIT and ROLLBACK
  - Checkpoint mechanism for log truncation
  - Automatic recovery on database startup

- **Index Support**: B+Tree indexes for improved query performance
  - CREATE INDEX support
  - Automatic index maintenance on INSERT/UPDATE/DELETE
  - Index usage in SELECT queries for equality conditions
  - Primary key lookup via index

- **JOIN Support**: Basic INNER JOIN functionality
  - JOIN with ON clause
  - Multi-table joins
  - Column qualification with table prefixes

- **Improved Data Persistence**:
  - WAL checkpoint on database close
  - Better durability guarantees

## [v1.0.1] - 2026-03-01

### Added
- **Aggregate Functions**: Complete support for:
  - COUNT(*), COUNT(column)
  - SUM(column)
  - AVG(column)
  - MIN(column)
  - MAX(column)
  - Works with WHERE clause filtering

- **WHERE Clause Enhancements**:
  - LIKE operator (pattern matching with % and _)
  - IN operator (column IN (1, 2, 3))
  - BETWEEN operator (column BETWEEN 1 AND 10)
  - NOT LIKE, NOT IN, NOT BETWEEN support

- **Query Modifiers**:
  - ORDER BY (ASC/DESC)
  - LIMIT
  - OFFSET
  - DISTINCT

- **GROUP BY**: Group query results by columns
  - GROUP BY with aggregate functions
  - GROUP BY with ORDER BY
  - GROUP BY with LIMIT

- **HAVING**: Filter grouped results
  - Works with all aggregate functions in HAVING clause

## [v1.0.0] - 2026-03-01

### Added
- **SQL Parser**: Full SQL parser with support for:
  - DDL: CREATE TABLE, CREATE INDEX, DROP TABLE
  - DML: SELECT, INSERT, UPDATE, DELETE
  - Transactions: BEGIN, COMMIT, ROLLBACK

- **WHERE Clause**: Complete WHERE clause support with:
  - Comparison operators: =, !=, <, >, <=, >=
  - NULL checks: IS NULL, IS NOT NULL
  - AND/OR logical operators

- **Placeholder Support**: Prepared statement placeholders (?) with proper index handling

- **Disk Persistence**: Data survives database restart
  - Schema persistence
  - Data persistence
  - Base64 encoding for binary data

- **In-Memory Mode**: RAM-only databases for testing/caching

- **Expression Evaluation**: Full expression evaluation for WHERE clauses

- **CLI Tool**: Command-line interface with interactive mode
  - In-memory and disk database support
  - SQL execution
  - Help commands

- **Benchmark Tool**: Performance testing CLI
  - INSERT, SELECT, UPDATE, DELETE, Transaction benchmarks
  - Configurable row counts

- **Comprehensive Tests**: Test coverage for core packages
  - Engine tests
  - Catalog tests
  - Server tests
  - Wire protocol tests
  - BTree tests
  - Integration tests

### Changed
- Improved INSERT handling to properly map columns to table schema
- Fixed placeholder indexing for multiple values
- Fixed email values not being stored correctly
- Optimized SELECT to properly extract selected columns

### Fixed
- Placeholder index bug causing incorrect values
- Email field showing wrong values
- Disk persistence not loading data on restart
- UPDATE with WHERE clause not filtering rows
- DELETE with WHERE clause not filtering rows
- UPDATE not applying new values correctly
- CREATE INDEX not supported in Exec()

## [v0.1.0] - 2026-02-28

### Added
- Initial release
- Basic SQL parser
- In-memory storage engine
- B+Tree implementation
- Buffer pool
- TCP server with wire protocol
- JSON support

---

## Roadmap (v1.1+)

### Planned Features
- [ ] WAL (Write-Ahead Log) for crash recovery
- [ ] B+Tree disk persistence
- [ ] Index usage in query execution (indexes created but not used in queries)
- [x] Query optimizer
- [x] SQL functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] JOIN support
- [ ] Foreign keys
- [ ] Table constraints
- [ ] More data types (DATE, TIMESTAMP, etc.)
- [ ] Performance optimizations

# Changelog

All notable changes to CobaltDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.0.0] - 2026-02-28

### Added
- **SQL Parser**: Full SQL parser with support for:
  - DDL: CREATE TABLE, CREATE INDEX, DROP TABLE
  - DML: SELECT, INSERT, UPDATE, DELETE
  - Transactions: BEGIN, COMMIT, ROLLBACK

- **WHERE Clause**: Complete WHERE clause support with:
  - Comparison operators: =, !=, <, >, <=, >=
  - NULL checks: IS NULL, IS NOT NULL

- **Placeholder Support**: Prepared statement placeholders (?) with proper index handling

- **Disk Persistence**: Data survives database restart
  - Schema persistence
  - Data persistence
  - Base64 encoding for binary data

- **In-Memory Mode**: RAM-only databases for testing/caching

- **Expression Evaluation**: Full expression evaluation for WHERE clauses

### Changed
- Improved INSERT handling to properly map columns to table schema
- Fixed placeholder indexing for multiple values
- Fixed email values not being stored correctly
- Optimized SELECT to properly extract selected columns

### Fixed
- Placeholder index bug causing incorrect values
- Email field showing wrong values
- Disk persistence not loading data on restart

## [v0.1.0] - 2026-XX-XX

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
- [ ] Index usage in query execution
- [ ] Query optimizer
- [ ] SQL functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] JOIN support
- [ ] Foreign keys
- [ ] Table constraints
- [ ] More data types (DATE, TIMESTAMP, etc.)
- [ ] Performance optimizations

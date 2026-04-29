# Contributing to CobaltDB

Thank you for your interest in contributing to CobaltDB! This guide covers the basics for getting started.

## Prerequisites

- Go 1.22+ (see `go.mod` for exact version)
- Git

## Development Setup

```bash
git clone https://github.com/cobaltdb/cobaltdb.git
cd cobaltdb
make build
make verify
```

## Branch Naming

- `feature/<short-description>` — new features
- `fix/<short-description>` — bug fixes
- `refactor/<short-description>` — code refactoring
- `perf/<short-description>` — performance improvements
- `docs/<short-description>` — documentation changes

## Commit Messages

Use conventional commit format:

```
type(scope): description

[optional body]
```

Types: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `ci`, `chore`

## Pull Request Process

1. Create a branch from `main`
2. Make your changes with tests
3. Ensure all tests pass: `make verify`
4. Ensure 90%+ coverage on changed packages
5. Open a PR against `main` with a clear description

## Testing

```bash
make test              # Unit tests
make verify            # Build + vet + test
make test-coverage     # Coverage report
make race              # Race detector (requires CGO)
```

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Write tests first (TDD preferred)
- Use existing error types (`ErrTableExists`, `ErrTableNotFound`, etc.)
- See `CLAUDE.md` for project-specific guidelines

## Reporting Issues

- Use GitHub Issues for bugs and feature requests
- Include reproduction steps, Go version, and OS
- For security vulnerabilities, see [SECURITY.md](SECURITY.md)

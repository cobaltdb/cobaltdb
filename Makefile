.PHONY: build test clean install test-coverage bench lint fmt deps run-server run-cli docker-build docker-run race vuln gosec verify verify-security all

BINARY_SERVER=cobaltdb-server
BINARY_CLI=cobaltdb-cli
PKG=github.com/cobaltdb/cobaltdb
SECURITY_PKGS=./cmd/cobaltdb-server ./pkg/server ./pkg/protocol ./pkg/storage ./pkg/auth ./sdk/go ./pkg/logger ./pkg/query

build:
	@echo "Building server..."
	@go build -o bin/$(BINARY_SERVER) $(PKG)/cmd/$(BINARY_SERVER)
	@echo "Building CLI..."
	@go build -o bin/$(BINARY_CLI) $(PKG)/cmd/$(BINARY_CLI)
	@echo "Build complete!"

test:
	@echo "Running tests..."
	@go test -v ./...

race:
	@echo "Running race detector (requires CGO toolchain)..."
	@CGO_ENABLED=1 go test -race ./...

vuln:
	@echo "Running govulncheck..."
	@govulncheck ./...

gosec:
	@echo "Running gosec..."
	@gosec -exclude=G115,G118,G304,G301,G306,G401,G402,G505 $(SECURITY_PKGS)

test-coverage:
	@echo "Running tests with coverage..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f coverage.out coverage.html
	@echo "Clean complete!"

install:
	@echo "Installing..."
	@go install $(PKG)/cmd/$(BINARY_SERVER)
	@go install $(PKG)/cmd/$(BINARY_CLI)
	@echo "Installation complete!"

lint:
	@echo "Running linter..."
	@golangci-lint run --tests=false --disable-all --enable=errcheck --enable=govet $(SECURITY_PKGS)

fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Format complete!"

deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies updated!"

verify:
	@echo "Running core verification..."
	@go build ./...
	@go vet ./...
	@go test ./...

verify-security:
	@echo "Running security and concurrency verification..."
	@$(MAKE) verify
	@$(MAKE) race
	@$(MAKE) vuln
	@$(MAKE) gosec
	@$(MAKE) lint

run-server:
	@go run ./cmd/$(BINARY_SERVER)

run-cli:
	@go run ./cmd/$(BINARY_CLI)

docker-build:
	@docker build -t cobaltdb:latest .

docker-run:
	@docker run -p 4200:4200 cobaltdb:latest

all: deps fmt test build

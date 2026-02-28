.PHONY: build test clean install

BINARY_SERVER=cobaltdb-server
BINARY_CLI=cobaltdb-cli
PKG=github.com/cobaltdb/cobaltdb

build:
	@echo "Building server..."
	@go build -o bin/$(BINARY_SERVER) $(PKG)/cmd/$(BINARY_SERVER)
	@echo "Building CLI..."
	@go build -o bin/$(BINARY_CLI) $(PKG)/cmd/$(BINARY_CLI)
	@echo "Build complete!"

test:
	@echo "Running tests..."
	@go test -v ./...

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
	@golangci-lint run ./...

fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Format complete!"

deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies updated!"

run-server:
	@go run ./cmd/$(BINARY_SERVER)

run-cli:
	@go run ./cmd/$(BINARY_CLI)

docker-build:
	@docker build -t cobaltdb:latest .

docker-run:
	@docker run -p 4200:4200 cobaltdb:latest

all: deps fmt test build

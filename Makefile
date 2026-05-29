.PHONY: build test clean install test-coverage bench lint staticcheck fmt fmt-check deps run-server run-cli docker-build docker-run race vuln gosec verify verify-security all

BINARY_SERVER=cobaltdb-server
BINARY_CLI=cobaltdb-cli
PKG=github.com/cobaltdb/cobaltdb
VERSION?=$(shell cat VERSION 2>/dev/null || echo dev)
LDFLAGS=-ldflags "-s -w -X main.version=$(VERSION)"

build:
	@echo "Building server..."
	@go build $(LDFLAGS) -o bin/$(BINARY_SERVER) $(PKG)/cmd/$(BINARY_SERVER)
	@echo "Building CLI..."
	@go build $(LDFLAGS) -o bin/$(BINARY_CLI) $(PKG)/cmd/$(BINARY_CLI)
	@echo "Build complete!"

test:
	@echo "Running tests..."
	@go test -v ./...

race:
	@echo "Running race detector (requires CGO toolchain)..."
	@CGO_ENABLED=1 go test -race ./...

vuln:
	@echo "Running govulncheck..."
	@go run golang.org/x/vuln/cmd/govulncheck@latest ./...

gosec:
	@echo "Running gosec..."
	@go run github.com/securego/gosec/v2/cmd/gosec@latest -exclude=G104 ./...

test-coverage:
	@echo "Running tests with coverage..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

bench-gate:
	@echo "Running benchmark regression gate..."
	@./scripts/benchmark-gate.sh

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

staticcheck:
	@echo "Running staticcheck..."
	@go run honnef.co/go/tools/cmd/staticcheck@latest ./...

fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Format complete!"

fmt-check:
	@echo "Checking gofmt..."
	@unformatted="$$(gofmt -l ./pkg ./cmd ./sdk ./integration ./test 2>/dev/null)"; \
	if [ -n "$$unformatted" ]; then \
		echo "The following files are not gofmt-clean:"; \
		echo "$$unformatted"; \
		echo "Run 'make fmt' to fix."; \
		exit 1; \
	fi
	@echo "gofmt clean."

deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies updated!"

verify:
	@echo "Running core verification..."
	@$(MAKE) fmt-check
	@go build ./...
	@go vet ./...
	@go test ./...

verify-security:
	@echo "Running security and concurrency verification..."
	@$(MAKE) verify
	@$(MAKE) race
	@$(MAKE) vuln
	@$(MAKE) gosec
	@$(MAKE) staticcheck

run-server:
	@go run ./cmd/$(BINARY_SERVER)

run-cli:
	@go run ./cmd/$(BINARY_CLI)

docker-build:
	@docker build -t cobaltdb:latest .

docker-run:
	@docker run -p 4200:4200 cobaltdb:latest

release:
	@echo "Building release binaries..."
	@mkdir -p dist
	@for goos in linux darwin windows; do \
		for goarch in amd64 arm64; do \
			if [ "$$goos" = "windows" ] && [ "$$goarch" = "arm64" ]; then continue; fi; \
			ext=""; if [ "$$goos" = "windows" ]; then ext=".exe"; fi; \
			CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch go build \
				-ldflags="-s -w -X main.version=$$(git describe --tags --always)" \
				-o "dist/$(BINARY_SERVER)-$$goos-$$goarch$$ext" \
				$(PKG)/cmd/$(BINARY_SERVER); \
			CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch go build \
				-ldflags="-s -w -X main.version=$$(git describe --tags --always)" \
				-o "dist/$(BINARY_CLI)-$$goos-$$goarch$$ext" \
				$(PKG)/cmd/$(BINARY_CLI); \
		done \
	done
	@cd dist && sha256sum * > checksums.txt
	@echo "Release artifacts built in dist/"
	@echo "Checksums:"
	@cat dist/checksums.txt

all: deps fmt test build

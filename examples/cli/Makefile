.PHONY: all build test clean run docker

BINARY_NAME=cobaltdb-server
CLI_NAME=cobaltdb-cli
DOCKER_IMAGE=cobaltdb:latest

all: test build

build:
	go build -o $(BINARY_NAME) ./cmd/cobaltdb-server
	go build -o $(CLI_NAME) ./cmd/cobaltdb-cli

test:
	go test ./... -v -race

test-coverage:
	go test ./... -race -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run

run: build
	./$(BINARY_NAME)

clean:
	rm -f $(BINARY_NAME) $(CLI_NAME) coverage.out coverage.html
	go clean

deps:
	go mod download
	go mod tidy

fmt:
	go fmt ./...

vet:
	go vet ./...

docker:
	docker build -t $(DOCKER_IMAGE) .

docker-run:
	docker-compose up -d

docker-stop:
	docker-compose down

examples:
	@echo "Building examples..."
	go build -o examples/rest-api/server ./examples/rest-api
	go build -o examples/cli/cli-tool ./examples/cli
	go build -o examples/worker/worker-app ./examples/worker
	go build -o examples/webapp/web ./examples/webapp

.DEFAULT_GOAL := all

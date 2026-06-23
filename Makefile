.PHONY: all build run test clean fmt lint

# Binary names
DAEMON_BINARY=bin/tumbleweed
CLI_BINARY=bin/tumbleweed-cli

all: build

build:
	@echo "Building Tumbleweed daemon and CLI..."
	@mkdir -p bin
	go build -o $(DAEMON_BINARY) ./cmd/tumbleweed
	go build -o $(CLI_BINARY) ./cmd/tumbleweed-cli
	@echo "Binaries created in bin/"

run: build
	@echo "Starting Tumbleweed daemon..."
	./$(DAEMON_BINARY)

test:
	@echo "Running all tests..."
	go test -v ./...

clean:
	@echo "Cleaning binaries and test data..."
	rm -rf bin/
	rm -rf data/

fmt:
	@echo "Formatting code..."
	go fmt ./...

lint:
	@echo "Linting code..."
	go vet ./...

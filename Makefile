.PHONY: all build run test clean fmt lint docker-build docker-run docker-stop

# Binary names
DAEMON_BINARY=bin/tumbleweed
CLI_BINARY=bin/tumbleweed-cli

# Docker settings
DOCKER_IMAGE=tumbleweed
DOCKER_CONTAINER=tumbleweed

all: build

build:
	@echo "Building Tumbleweed daemon..."
	@mkdir -p bin
	go build -o $(DAEMON_BINARY) ./cmd/tumbleweed
	@echo "Building Tumbleweed CLI client..."
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

docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE):latest .

docker-run:
	@echo "Running Docker container..."
	docker run -d \
		-p 8765:8765 \
		-v $$(pwd)/data:/app/data \
		--name $(DOCKER_CONTAINER) \
		$(DOCKER_IMAGE):latest

docker-stop:
	@echo "Stopping and removing Docker container..."
	-docker stop $(DOCKER_CONTAINER)
	-docker rm $(DOCKER_CONTAINER)

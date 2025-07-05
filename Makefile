# KaflowSQL Makefile

# Variables
BINARY_NAME=kaflowsql
ENGINE_BINARY=engine
VERSION?=$(shell git describe --tags --dirty --always)
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags="-w -s -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"
PLATFORMS=linux/amd64,linux/arm64,linux/arm/v7,darwin/amd64,darwin/arm64,windows/amd64

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Default target
.PHONY: all
all: clean build

# Help target
.PHONY: help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build targets
.PHONY: build
build: ## Build the KaflowSQL engine
	@echo "Building $(ENGINE_BINARY)..."
	CGO_ENABLED=1 $(GOBUILD) $(LDFLAGS) -o bin/$(ENGINE_BINARY) cmd/engine/main.go

.PHONY: build-cross
build-cross: ## Build binaries for multiple platforms
	@echo "Building for multiple platforms..."
	@for platform in $(shell echo $(PLATFORMS) | tr "," "\n"); do \
		GOOS=$$(echo $$platform | cut -d'/' -f1); \
		GOARCH=$$(echo $$platform | cut -d'/' -f2); \
		GOARM=$$(echo $$platform | cut -d'/' -f3); \
		if [ "$$GOARM" != "$$GOARCH" ]; then \
			export GOARM=$$GOARM; \
		fi; \
		echo "Building for $$GOOS/$$GOARCH$$GOARM..."; \
		CGO_ENABLED=1 GOOS=$$GOOS GOARCH=$$GOARCH $(GOBUILD) $(LDFLAGS) -o dist/$(ENGINE_BINARY)-$$GOOS-$$GOARCH cmd/engine/main.go; \
	done

# Test targets
.PHONY: test
test: ## Run unit tests
	$(GOTEST) -v -short ./...

.PHONY: test-all
test-all: ## Run all tests including integration tests
	$(GOTEST) -v ./...

.PHONY: test-integration
test-integration: ## Run integration tests only
	$(GOTEST) -v -tags=integration ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	$(GOTEST) -v -short -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

.PHONY: test-coverage-full
test-coverage-full: ## Run all tests with coverage
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	$(GOCMD) tool cover -func=coverage.out

.PHONY: test-race
test-race: ## Run tests with race detector
	$(GOTEST) -v -race -short ./...

.PHONY: test-bench
test-bench: ## Run benchmarks
	$(GOTEST) -v -bench=. -benchmem -short ./...

.PHONY: test-performance
test-performance: ## Run performance benchmarks
	$(GOTEST) -v -bench=. -benchmem -benchtime=10s ./...

.PHONY: test-verbose
test-verbose: ## Run tests with verbose output
	$(GOTEST) -v -short -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -func=coverage.out

# Development targets
.PHONY: run
run: build ## Build and run KaflowSQL engine
	./bin/$(ENGINE_BINARY)

.PHONY: deps
deps: ## Download dependencies
	$(GOMOD) download
	$(GOMOD) tidy

.PHONY: deps-upgrade
deps-upgrade: ## Upgrade dependencies
	$(GOGET) -u ./...
	$(GOMOD) tidy

# Code quality targets
.PHONY: fmt
fmt: ## Format code
	$(GOCMD) fmt ./...

.PHONY: vet
vet: ## Run go vet
	$(GOCMD) vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	/Users/rafael.siqueira/go/bin/golangci-lint run

.PHONY: check
check: fmt vet lint test ## Run all code quality checks

# Docker targets
.PHONY: docker-build
docker-build: ## Build Docker image
	docker build -t $(BINARY_NAME):$(VERSION) .
	docker tag $(BINARY_NAME):$(VERSION) $(BINARY_NAME):latest

.PHONY: docker-build-multi
docker-build-multi: ## Build multi-platform Docker images
	docker buildx build --platform $(PLATFORMS) -t $(BINARY_NAME):$(VERSION) -t $(BINARY_NAME):latest .

.PHONY: docker-push
docker-push: docker-build ## Build and push Docker image
	docker push $(BINARY_NAME):$(VERSION)
	docker push $(BINARY_NAME):latest

.PHONY: docker-run
docker-run: ## Run Docker container
	docker run --rm -p 8080:8080 $(BINARY_NAME):latest

.PHONY: docker-compose-up
docker-compose-up: ## Start KaflowSQL with docker-compose
	docker-compose up -d

.PHONY: docker-compose-down
docker-compose-down: ## Stop KaflowSQL with docker-compose
	docker-compose down -v

.PHONY: docker-compose-dev
docker-compose-dev: ## Start development environment (includes Kafka)
	docker-compose -f docker-compose.dev.yml up -d

# Clean targets
.PHONY: clean
clean: ## Clean build artifacts
	$(GOCLEAN)
	rm -rf bin/
	rm -rf dist/
	rm -f coverage.out coverage.html

.PHONY: clean-docker
clean-docker: ## Clean Docker images and containers
	docker-compose down -v --remove-orphans
	docker rmi -f $(BINARY_NAME):latest $(BINARY_NAME):$(VERSION) 2>/dev/null || true

# Install targets
.PHONY: install
install: build ## Install binary to GOPATH/bin
	cp bin/$(ENGINE_BINARY) $(GOPATH)/bin/

# Release targets
.PHONY: release
release: clean test build-cross ## Prepare release artifacts
	@echo "Release $(VERSION) ready in dist/"

# Setup targets
.PHONY: setup-dev
setup-dev: ## Setup development environment
	@echo "Setting up development environment..."
	$(GOMOD) download
	@echo "Development environment ready!"

.PHONY: setup-hooks
setup-hooks: ## Setup git hooks
	@echo "Setting up git hooks..."
	cp scripts/pre-commit.sh .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "Git hooks installed!"
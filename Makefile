SHELL := /bin/sh

.DEFAULT_GOAL := all

# Binary and package configuration
BINARY_NAME := glowkron
CMD_PATH := ./cmd/glowkron

# Version and build flags
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

# Run arguments (defaults to daemon)
ARGS ?= daemon

.PHONY: all build install test test-race vet fmt run clean help

all: build ## Default target: build the GlowKron binary

build: ## Build zero-CGo binary to ./glowkron
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) $(CMD_PATH)

install: ## Install binary to GOBIN / GOPATH/bin
	CGO_ENABLED=0 go install -trimpath -ldflags "$(LDFLAGS)" $(CMD_PATH)

test: ## Run unit and integration tests
	go test -v -count=1 ./...

test-race: ## Run race detector tests across all packages
	go test -race -v -count=1 ./...

vet: ## Run go vet static analysis across all packages
	go vet ./...

fmt: ## Enforce canonical formatting using gofmt
	gofmt -w -s .

run: build ## Build binary and execute with ARGS (defaults to daemon)
	./$(BINARY_NAME) $(ARGS)

clean: ## Remove compiled binary and test cache
	rm -f $(BINARY_NAME)
	go clean -testcache

help: ## Show this help message
	@echo "GlowKron Build System"
	@echo "Usage: make [target] [ARGS=\"...\"]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

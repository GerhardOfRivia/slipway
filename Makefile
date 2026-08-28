# OUTPUT remains an alias for the client path for compatibility with existing
# build invocations.
OUTPUT ?= bin
SLIPWAY_OUTPUT ?= $(OUTPUT)/slipway
SLIPWAYD_OUTPUT ?= $(OUTPUT)/slipwayd
SEMVER ?= 1.0.1
VERSION ?= $(SEMVER)-dev
LDFLAGS = -ldflags "-X main.Version=$(VERSION)"
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: all build web build-web-docker build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 test clean

all: build

build-linux-amd64:
	$(MAKE) build GOOS=linux GOARCH=amd64 CGO_ENABLED=0 SLIPWAY_OUTPUT=bin/slipway-linux-amd64 SLIPWAYD_OUTPUT=bin/slipwayd-linux-amd64

build-linux-arm64:
	$(MAKE) build GOOS=linux GOARCH=arm64 CGO_ENABLED=0 SLIPWAY_OUTPUT=bin/slipway-linux-arm64 SLIPWAYD_OUTPUT=bin/slipwayd-linux-arm64

build-darwin-amd64:
	$(MAKE) build GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 SLIPWAY_OUTPUT=bin/slipway-darwin-amd64 SLIPWAYD_OUTPUT=bin/slipwayd-darwin-amd64

build-darwin-arm64:
	$(MAKE) build GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 SLIPWAY_OUTPUT=bin/slipway-darwin-arm64 SLIPWAYD_OUTPUT=bin/slipwayd-darwin-arm64

build:
	@echo "Building $(SLIPWAY_OUTPUT) for $(GOOS)/$(GOARCH) with version $(VERSION)..."
	mkdir -p $(dir $(SLIPWAY_OUTPUT))
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(LDFLAGS) -o $(SLIPWAY_OUTPUT) ./cmd/slipway
	@echo "Building $(SLIPWAYD_OUTPUT) for $(GOOS)/$(GOARCH) with version $(VERSION)..."
	mkdir -p $(dir $(SLIPWAYD_OUTPUT))
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(LDFLAGS) -o $(SLIPWAYD_OUTPUT) ./cmd/slipwayd

web:
	# npm ci && npm run build
	docker run --rm --user $$(id -u):$$(id -g) --mount type=bind,src=$(CURDIR),dst=/workspace -w /workspace/web node:22-bookworm-slim /bin/sh -lc 'npm ci && npm run build'
	rm -rf ./web/node_modules ./web/tsconfig.app.tsbuildinfo ./web/tsconfig.node.tsbuildinfo

test:
	@echo "Running tests..."
	go fmt ./...
	go vet ./...
	go test -v ./...

clean:
	@echo "Cleaning up..."
	rm -f $(SLIPWAY_OUTPUT) $(SLIPWAYD_OUTPUT)

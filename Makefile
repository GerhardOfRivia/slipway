# OUTPUT remains an alias for the client path for compatibility with existing
# build invocations.
OUTPUT ?= bin
SLIPWAY_OUTPUT ?= $(OUTPUT)/slipway
SLIPWAYD_OUTPUT ?= $(OUTPUT)/slipwayd
SEMVER ?= 1.0.2
VERSION ?= $(SEMVER)-dev
RELEASE_TAG ?= v$(SEMVER)
LDFLAGS = -ldflags "-X main.Version=$(VERSION)"
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: all build web build-web-docker build-linux-amd64 build-linux-arm64 test release clean

all: build

activate:
	source activate

build-linux-amd64:
	$(MAKE) build GOOS=linux GOARCH=amd64 CGO_ENABLED=0 SLIPWAY_OUTPUT=bin/slipway-linux-amd64 SLIPWAYD_OUTPUT=bin/slipwayd-linux-amd64

build-linux-arm64:
	$(MAKE) build GOOS=linux GOARCH=arm64 CGO_ENABLED=0 SLIPWAY_OUTPUT=bin/slipway-linux-arm64 SLIPWAYD_OUTPUT=bin/slipwayd-linux-arm64

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

release:
	@test -z "$$(git status --porcelain)" || { echo "Refusing to release with a dirty working tree."; exit 1; }
	@if git rev-parse --verify --quiet "refs/tags/$(RELEASE_TAG)" >/dev/null; then echo "Tag $(RELEASE_TAG) already exists."; exit 1; fi
	git tag "$(RELEASE_TAG)"
	git push origin "$(RELEASE_TAG)"

clean:
	@echo "Cleaning up..."
	rm -f $(SLIPWAY_OUTPUT) $(SLIPWAYD_OUTPUT)

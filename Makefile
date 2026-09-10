# OUTPUT remains an alias for the client path for compatibility with existing
# build invocations.
OUTPUT ?= bin
ONDERZEEER_OUTPUT ?= $(OUTPUT)/onderzeeer
ONDERZEEERD_OUTPUT ?= $(OUTPUT)/onderzeeerd
SEMVER ?= 1.1.1
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
	$(MAKE) build GOOS=linux GOARCH=amd64 CGO_ENABLED=0 ONDERZEEER_OUTPUT=bin/onderzeeer-linux-amd64 ONDERZEEERD_OUTPUT=bin/onderzeeerd-linux-amd64

build-linux-arm64:
	$(MAKE) build GOOS=linux GOARCH=arm64 CGO_ENABLED=0 ONDERZEEER_OUTPUT=bin/onderzeeer-linux-arm64 ONDERZEEERD_OUTPUT=bin/onderzeeerd-linux-arm64

build:
	@echo "Building $(ONDERZEEER_OUTPUT) for $(GOOS)/$(GOARCH) with version $(VERSION)..."
	mkdir -p $(dir $(ONDERZEEER_OUTPUT))
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(LDFLAGS) -o $(ONDERZEEER_OUTPUT) ./cmd/onderzeeer
	@echo "Building $(ONDERZEEERD_OUTPUT) for $(GOOS)/$(GOARCH) with version $(VERSION)..."
	mkdir -p $(dir $(ONDERZEEERD_OUTPUT))
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(LDFLAGS) -o $(ONDERZEEERD_OUTPUT) ./cmd/onderzeeerd

build-docker:
	docker build --pull \
	--build-arg ONDERZEEER_UID="$(id -u)" \
	--build-arg ONDERZEEER_GID="$(id -g)" \
	--build-arg VERSION="$(git describe --tags --always --dirty)" \
	-t localhost/onderzeeer:local .

build-podman:
	podman build --pull=always --format docker \
	--build-arg ONDERZEEER_UID="$(id -u)" \
	--build-arg ONDERZEEER_GID="$(id -g)" \
	--build-arg VERSION="$(git describe --tags --always --dirty)" \
	-t localhost/onderzeeer:local .

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
	rm -f $(ONDERZEEER_OUTPUT) $(ONDERZEEERD_OUTPUT)

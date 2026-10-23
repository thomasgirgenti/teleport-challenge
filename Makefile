GO_IMAGE  := golang:1.27
IMAGE     := workerd:dev
CONTAINER := workerd

.PHONY: test test-linux vet fmt tidy proto build build-native-cli certs docker-build docker-run docker-stop

default: fmt tidy proto build test-linux

# Run all tests with race detection. Requires Linux.
test:
	go test -race -count=1 ./...

# Run vet and tests in a Linux container for development on other platforms
# --init runs a real init as pid 1 so killed orphan processes get reaped
test-linux:
	docker run --rm --init -v "$(CURDIR)":/src -w /src \
		-v go-mod-cache:/go/pkg/mod -v go-build-cache:/root/.cache/go-build \
		$(GO_IMAGE) sh -c 'go vet ./... && go test -race -count=1 ./...'

vet:
	GOOS=linux go vet ./...

fmt:
	gofmt -l -w .

tidy:
	go mod tidy

# Lint the protobuf definitions and generate Go code from them. Requires buf; the Go plugins are pinned as tools in go.mod.
# buf must be installed locally: https://buf.build/docs/cli/installation/
proto:
	buf lint
	buf generate

# Build every binary into bin/. -trimpath and CGO_ENABLED=0 keep builds reproducible across machines.
build:
	CGO_ENABLED=0 GOOS=linux go build -trimpath -o bin/ ./cmd/...

# Build jobctl for the host OS, e.g. to talk to workerd running in Docker on macOS
build-native-cli:
	go build -o bin/jobctl ./cmd/jobctl

# Generate a development CA, server, and client certs into certs/. Re-running replaces them all.
certs:
	go run ./cmd/certgen

# Build the workerd image. See "Running on macOS with Docker" in README.md.
docker-build:
	docker build -t $(IMAGE) .

# Run workerd in the foreground with certs/ mounted read-only. Ctrl-C stops it and kills every job.
# The port is published on host loopback only, so the server isn't reachable from the network.
# --init forwards signals to workerd and reaps orphaned job processes.
docker-run: docker-build
	@test -f certs/server.crt || { echo "certs/ is missing: run 'make certs' first"; exit 1; }
	docker run --rm --init --name $(CONTAINER) \
		-p 127.0.0.1:50051:50051 \
		-v "$(CURDIR)/certs":/app/certs:ro \
		$(IMAGE)

# Stop the workerd container. SIGTERM, so the server kills its jobs and exits cleanly.
docker-stop:
	docker stop $(CONTAINER)

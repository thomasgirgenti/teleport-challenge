GO_IMAGE := golang:1.27

.PHONY: test test-linux vet fmt tidy proto build certs

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

# Generate a development CA, server, and client certs into certs/. Re-running replaces them all.
certs:
	go run ./cmd/certgen

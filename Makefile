GO_IMAGE := golang:1.27

.PHONY: test test-linux vet fmt tidy proto

default: fmt tidy test-linux

# Run all tests with race detection. Requires Linux.
test:
	go test -race -count=1 -timeout=10s ./...

# Run vet and tests in a Linux container for development on other platforms
# --init runs a real init as pid 1 so killed orphan processes get reaped
test-linux:
	docker run --rm --init -v "$(CURDIR)":/src -w /src \
		-v go-mod-cache:/go/pkg/mod -v go-build-cache:/root/.cache/go-build \
		$(GO_IMAGE) sh -c 'go vet ./... && go test -race -count=1 -timeout=10s ./...'

vet:
	GOOS=linux go vet ./...

fmt:
	gofmt -l -w .

tidy:
	go mod tidy

# Generate Go code from the protobuf definitions. Requires protoc; the Go plugins are pinned as tools in go.mod.
proto:
	protoc --proto_path=api \
		--plugin=protoc-gen-go="$$(go tool -n protoc-gen-go)" \
		--plugin=protoc-gen-go-grpc="$$(go tool -n protoc-gen-go-grpc)" \
		--go_out=api --go_opt=paths=source_relative \
		--go-grpc_out=api --go-grpc_opt=paths=source_relative \
		api/worker/v1/worker.proto

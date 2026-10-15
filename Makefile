GO_IMAGE := golang:1.27

.PHONY: test test-linux vet fmt

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

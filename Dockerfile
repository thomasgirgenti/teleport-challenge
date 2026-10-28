# Image for running workerd, e.g. on macOS where it can't run natively. See "Running on macOS with Docker" in README.md.
# Certs are not baked in: mount them read-only at /app/certs (make docker-run does this).

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 go build -trimpath -o /out/workerd ./cmd/workerd

# A full userland, so jobs can run bash, sleep, coreutils, etc.
FROM debian:trixie
WORKDIR /app
COPY --from=build /out/workerd /usr/local/bin/workerd
EXPOSE 50051
# Listen on all interfaces: Docker's port publishing can't reach the container's loopback
ENTRYPOINT ["workerd", "--addr", "0.0.0.0:50051"]

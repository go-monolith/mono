# Image of the k8s-cluster example. The example lives in the framework's own Go
# module, so the build context is the repository root:
#
#   docker build -f examples/k8s-cluster/Dockerfile -t mono-k8s-cluster:0.2.0 .
#
# Dockerfile.dockerignore (next to this file) keeps the context small.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/k8s-cluster ./examples/k8s-cluster

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/k8s-cluster /k8s-cluster
# 4222 NATS clients, 6222 NATS cluster routes, 8222 NATS monitoring, 8080 HTTP API
EXPOSE 4222 6222 8222 8080
USER nonroot:nonroot
ENTRYPOINT ["/k8s-cluster"]

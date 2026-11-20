# One image for every platform binary: gateway, applier, devicesim, verify, eipctl.
# deploy/k8s and deploy/aws pick the binary with the container command.
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/gateway ./cmd/applier ./cmd/devicesim ./cmd/verify ./cmd/eipctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/gateway"]

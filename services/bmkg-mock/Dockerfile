# Build context is the repository root:  docker build -f services/bmkg-mock/Dockerfile .
FROM golang:1.25-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY pkg/obs ./pkg/obs
COPY services/bmkg-mock ./services/bmkg-mock
WORKDIR /src/services/bmkg-mock
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/bmkg-mock ./cmd/bmkg-mock
FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/bmkg-mock /usr/local/bin/bmkg-mock
USER app
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/bmkg-mock"]

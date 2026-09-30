# Build context is the repository root:  docker build -f services/aggregator/Dockerfile .
FROM golang:1.25-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY pkg/obs ./pkg/obs
COPY services/aggregator ./services/aggregator
WORKDIR /src/services/aggregator
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/aggregator ./cmd/aggregator

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/aggregator /usr/local/bin/aggregator
COPY services/aggregator/config /app/config
USER app
EXPOSE 8083
ENTRYPOINT ["/usr/local/bin/aggregator"]

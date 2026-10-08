# Build context is the repository root:  docker build -f services/consumer-hazard/Dockerfile .
FROM golang:1.25-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY pkg/obs ./pkg/obs
COPY services/consumer-hazard ./services/consumer-hazard
WORKDIR /src/services/consumer-hazard
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/consumer-hazard ./cmd/consumer-hazard

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/consumer-hazard /usr/local/bin/consumer-hazard
USER app
EXPOSE 8091
ENTRYPOINT ["/usr/local/bin/consumer-hazard"]

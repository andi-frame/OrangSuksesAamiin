# Build context is the repository root:  docker build -f services/client-api/Dockerfile .
FROM golang:1.25-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY pkg/obs ./pkg/obs
COPY services/client-api ./services/client-api
WORKDIR /src/services/client-api
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/client-api ./cmd/client-api

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/client-api /usr/local/bin/client-api
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/client-api"]

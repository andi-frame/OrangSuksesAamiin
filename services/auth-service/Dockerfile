# Build context is the repository root:  docker build -f services/auth-service/Dockerfile .
FROM golang:1.25-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY pkg/obs ./pkg/obs
COPY services/auth-service ./services/auth-service
WORKDIR /src/services/auth-service
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/auth-service ./cmd/auth-service

FROM alpine:3.20
RUN adduser -D -u 10001 app && mkdir -p /data && chown app:app /data
COPY --from=build /out/auth-service /usr/local/bin/auth-service
USER app
EXPOSE 8084
ENTRYPOINT ["/usr/local/bin/auth-service"]

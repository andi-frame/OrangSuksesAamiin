# Build context is the repository root:  docker build -f services/pvmbg-mock/Dockerfile .
FROM golang:1.25-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY pkg/obs ./pkg/obs
COPY services/pvmbg-mock ./services/pvmbg-mock
WORKDIR /src/services/pvmbg-mock
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/pvmbg-mock ./cmd/pvmbg-mock

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/pvmbg-mock /usr/local/bin/pvmbg-mock
USER app
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/pvmbg-mock"]

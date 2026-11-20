# Build context is the repository root:  docker build -f services/field-reports/Dockerfile .
FROM golang:1.25-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY pkg/obs ./pkg/obs
COPY services/field-reports ./services/field-reports
WORKDIR /src/services/field-reports
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/field-reports ./cmd/field-reports

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/field-reports /usr/local/bin/field-reports
USER app
EXPOSE 8093
ENTRYPOINT ["/usr/local/bin/field-reports"]

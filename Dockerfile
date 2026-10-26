FROM migrate/migrate:v4.20.1 AS migrations

COPY migrations/ /migrations/
COPY docker/migrate.sh /usr/local/bin/relay-migrate

ENTRYPOINT ["/bin/sh", "/usr/local/bin/relay-migrate"]
CMD ["up"]

FROM golang:1.26.5-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal

ENV CGO_ENABLED=0 GOOS=linux

RUN go build -trimpath -ldflags="-s -w" \
    -o /out/api ./cmd/api \
&& go build -trimpath -ldflags="-s -w" \
    -o /out/worker ./cmd/worker \
&& go build -trimpath -ldflags="-s -w" \
    -o /out/consumer ./cmd/consumer

FROM alpine:3.22 AS app

RUN apk add --no-cache ca-certificates \
&& addgroup -S relay \
&& adduser -S -G relay relay

WORKDIR /app

COPY --from=builder /out/api /app/api
COPY --from=builder /out/worker /app/worker
COPY --from=builder /out/consumer /app/consumer

USER relay

EXPOSE 8080

CMD ["/app/api"]

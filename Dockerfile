# One Dockerfile builds any of the three services, selected by the SERVICE build
# arg (content-service | review-service | user-service). Each compose service
# passes its own SERVICE, so all three share this build recipe and layer cache.

# ---- Build stage ----
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Cache module downloads separately from source for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
# CGO disabled -> a fully static binary (kafka-go and grpc are pure Go).
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/app ./cmd/${SERVICE}

# ---- Runtime stage ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /out/app /app/app
USER app
ENTRYPOINT ["/app/app"]

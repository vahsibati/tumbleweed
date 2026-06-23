# Build Stage
FROM golang:alpine AS builder

# Install build dependencies if needed (e.g., git, ca-certificates)
RUN apk update && apk add --no-cache git ca-certificates

WORKDIR /app

# Copy dependency definition
COPY go.mod ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bin/tumbleweed ./cmd/tumbleweed

# Final Runner Stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/bin/tumbleweed /app/tumbleweed

# Expose default Tumbleweed port
EXPOSE 8765

# Mount point for topics write-ahead log & state metadata
VOLUME ["/app/data"]

# Run the Tumbleweed daemon
ENTRYPOINT ["/app/tumbleweed"]
CMD ["-data-dir", "/app/data", "-bind", ":8765"]

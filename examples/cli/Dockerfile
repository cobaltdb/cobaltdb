# Build stage
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cobaltdb-server ./cmd/cobaltdb-server

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /build/cobaltdb-server .

RUN mkdir -p /data

EXPOSE 14200 3307 8080

VOLUME ["/data"]

ENTRYPOINT ["./cobaltdb-server"]
CMD ["-data", "/data"]

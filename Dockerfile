FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o s3fs .

FROM alpine:latest
COPY --from=builder /app/s3fs /s3fs
ENTRYPOINT ["/s3fs"]

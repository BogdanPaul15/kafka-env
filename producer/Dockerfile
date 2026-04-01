FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o producer cmd/main.go

FROM alpine:latest

RUN apk add --no-cache netcat-openbsd

WORKDIR /app

COPY --from=builder /app/producer .

EXPOSE 8080

CMD ["./producer"]
# Migrado de php:8.2-cli-alpine (Vulnerável em todas as versões do PHP) para Go 1.22 (Binário Nativo Memory-Safe)
FROM golang:1.22-alpine
RUN apk add --no-cache python3
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o notification-server main.go
EXPOSE 3007
CMD ["./notification-server"]

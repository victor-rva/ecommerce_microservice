FROM golang:1.22-alpine
RUN apk add --no-cache python3
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o gateway main.go
EXPOSE 3000
CMD ["./gateway"]

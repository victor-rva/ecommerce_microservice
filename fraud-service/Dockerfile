FROM rust:1.80-alpine
RUN apk add --no-cache musl-dev python3
WORKDIR /app
COPY src ./src
RUN rustc -O src/main.rs -o fraud-server
EXPOSE 3005
CMD ["./fraud-server"]

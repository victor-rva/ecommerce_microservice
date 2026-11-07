FROM clojure:temurin-17-tools-deps-alpine
RUN apk add --no-cache python3
WORKDIR /app
COPY deps.edn ./
COPY src ./src
EXPOSE 3003
CMD ["clojure", "-M:run"]

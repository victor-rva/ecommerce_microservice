FROM eclipse-temurin:17-jdk-alpine
RUN apk add --no-cache python3
WORKDIR /app
COPY src ./src
EXPOSE 3006
CMD ["java", "src/main/java/com/ecommerce/payment/PaymentApplication.java"]

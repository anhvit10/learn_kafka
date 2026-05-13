# ───────────────────────────────
# Stage 1: Build
# ───────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app

# copy pom.xml trước -> cache dependencies
# chỉ khi pom.xml thay đổi mới download lại
COPY pom.xml .
RUN mvn dependency:go-offline -B

# copy source rồi build
COPY src ./src
RUN mvn clean package -DskipTests

# ───────────────────────────────
# Stage 2: Runtime
# ───────────────────────────────
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# tạo user riêng, không chạy bằng root
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# copy jar từ stage build
COPY --from=builder /app/target/*.jar app.jar

# port Spring Boot
EXPOSE 8080

# chạy app
ENTRYPOINT ["java", "-jar", "app.jar"]
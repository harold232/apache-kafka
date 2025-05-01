# Etapa 1: Compilar el proyecto con Maven
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /app

# Copiar archivos del proyecto
COPY pom.xml .
COPY src ./src

# Compilar el proyecto y generar el JAR
RUN mvn clean package -DskipTests

# Etapa 2: Imagen final con solo Java y el JAR
FROM eclipse-temurin:17-jdk

WORKDIR /app

# Copiar el JAR generado desde la etapa anterior
COPY --from=builder /app/target/kafka-demo-1.0-SNAPSHOT.jar app.jar

# Comando por defecto (se puede sobrescribir)
CMD ["java", "-cp", "app.jar", "com.harold.KafkaProducerDemo"]

# Builds the job jar with Maven, then adds it to the Flink image.
# Run the tests before the build with "mvn test". They read the Iceberg
# migrations in ../hsec-db-iceberg, which is outside this build context.
FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

FROM flink:2.2.1-scala_2.12-java17
COPY --from=build /src/target/hsec-stream.jar /opt/flink/usrlib/hsec-stream.jar

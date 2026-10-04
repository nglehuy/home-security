# Builds the job jar with Maven, gets the library jars, and adds both to the Spark image.
# Run the tests before the build with "mvn test". One test reads the ClickHouse
# migrations in ../hsec-db-clickhouse, which is outside this build context.
FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

# Iceberg, its S3 client, the Postgres driver for the catalog, and the ClickHouse connector.
ARG COPY="mvn -B -q org.apache.maven.plugins:maven-dependency-plugin:3.9.0:copy -DoutputDirectory=/jars -Dartifact"
RUN ${COPY}=org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0 \
 && ${COPY}=org.apache.iceberg:iceberg-aws-bundle:1.12.0 \
 && ${COPY}=org.postgresql:postgresql:42.7.13 \
 && ${COPY}=com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.10.0 \
 && ${COPY}=com.clickhouse:clickhouse-jdbc:0.9.5:jar:all

FROM apache/spark:4.0.4-scala2.13-java17-ubuntu
COPY --from=build /jars/ /opt/spark/jars/
COPY --from=build /src/target/hsec-batch.jar /opt/hsec/hsec-batch.jar

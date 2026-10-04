package hsec.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalTime;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BatchConfigTest {
    static final String[] ARGS = {
        "--timezone", "Asia/Singapore",
        "--night-start", "22:30",
        "--catalog-uri", "jdbc:postgresql://postgres.hsec.svc.cluster.local:5432/iceberg_catalog",
        "--warehouse", "s3://hsec-lake/warehouse",
        "--s3-endpoint", "http://rustfs-svc.hsec.svc.cluster.local:9000",
        "--clickhouse-host", "clickhouse.hsec.svc.cluster.local",
    };
    static final Map<String, String> ENV = Map.of("POSTGRES_ICEBERG_PASSWORD", "pg", "RUSTFS_ACCESS_KEY", "ak",
            "RUSTFS_SECRET_KEY", "sk", "CLICKHOUSE_SPARK_PASSWORD", "ch");

    @Test
    void buildsBothCatalogs() {
        BatchConfig c = BatchConfig.fromArgs(ARGS, ENV);
        assertThat(c.nightStart).isEqualTo(LocalTime.of(22, 30));
        assertThat(c.nightEnd).isEqualTo(LocalTime.of(6, 0));
        assertThat(c.sparkConf())
                .containsEntry("spark.sql.session.timeZone", "Asia/Singapore")
                .containsEntry("spark.sql.catalog.lake.type", "jdbc")
                .containsEntry("spark.sql.catalog.lake.jdbc.password", "pg")
                .containsEntry("spark.sql.catalog.lake.s3.secret-access-key", "sk")
                .containsEntry("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
                .containsEntry("spark.sql.catalog.clickhouse.host", "clickhouse.hsec.svc.cluster.local")
                .containsEntry("spark.sql.catalog.clickhouse.http_port", "8123")
                .containsEntry("spark.sql.catalog.clickhouse.user", "spark_writer")
                .containsEntry("spark.sql.catalog.clickhouse.password", "ch");
    }

    @Test
    void failsWithoutTheClickHousePassword() {
        BatchConfig c = BatchConfig.fromArgs(ARGS, Map.of("POSTGRES_ICEBERG_PASSWORD", "pg",
                "RUSTFS_ACCESS_KEY", "ak", "RUSTFS_SECRET_KEY", "sk"));
        assertThatThrownBy(c::sparkConf).hasMessageContaining("CLICKHOUSE_SPARK_PASSWORD");
    }
}

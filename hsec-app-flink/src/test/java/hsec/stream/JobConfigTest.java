package hsec.stream;

import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class JobConfigTest {
    private static final String[] ARGS = {
        "--timezone", "Asia/Singapore",
        "--frigate-url", "https://192.168.1.17:8971/",
        "--frigate-api", "http://frigate-api.hsec.svc.cluster.local:5000",
        "--kafka-bootstrap", "redpanda-0.redpanda.hsec.svc.cluster.local:9093",
        "--catalog-uri", "jdbc:postgresql://postgres.hsec.svc.cluster.local:5432/iceberg_catalog",
        "--warehouse", "s3://hsec-lake/warehouse",
        "--s3-endpoint", "http://rustfs-svc.hsec.svc.cluster.local:9000",
    };
    private static final Map<String, String> ENV = Map.of(
            "POSTGRES_ICEBERG_PASSWORD", "pg", "RUSTFS_ACCESS_KEY", "ak", "RUSTFS_SECRET_KEY", "sk");

    @Test
    void readsArgumentsAndPasswords() {
        JobConfig c = JobConfig.fromArgs(ARGS, ENV);
        assertThat(c.zone).isEqualTo(ZoneId.of("Asia/Singapore"));
        assertThat(c.frigateUrl).isEqualTo("https://192.168.1.17:8971");
        assertThat(c.catalogProperties)
                .containsEntry("uri", "jdbc:postgresql://postgres.hsec.svc.cluster.local:5432/iceberg_catalog")
                .containsEntry("jdbc.user", "iceberg")
                .containsEntry("jdbc.password", "pg")
                .containsEntry("warehouse", "s3://hsec-lake/warehouse")
                .containsEntry("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.access-key-id", "ak")
                .containsEntry("s3.secret-access-key", "sk")
                .containsEntry("client.region", "us-east-1");
    }

    @Test
    void failsWithoutAPassword() {
        assertThatThrownBy(() -> JobConfig.fromArgs(ARGS, Map.of("RUSTFS_ACCESS_KEY", "ak")))
                .hasMessageContaining("POSTGRES_ICEBERG_PASSWORD");
    }

    @Test
    void failsOnAMissingValue() {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[] {"--timezone"}, ENV))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

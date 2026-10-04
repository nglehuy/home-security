package hsec.batch;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Settings of the batch job. The ScheduledSparkApplication passes them as arguments, and the passwords come from spark-env. */
public final class BatchConfig {
    public final ZoneId zone;
    public final LocalTime nightStart;
    public final LocalTime nightEnd;
    private final Map<String, String> args;
    private final Map<String, String> env;

    private BatchConfig(Map<String, String> args, Map<String, String> env) {
        this.args = args;
        this.env = env;
        this.zone = ZoneId.of(require(args, "timezone"));
        this.nightStart = LocalTime.parse(args.getOrDefault("night-start", "23:00"));
        this.nightEnd = LocalTime.parse(args.getOrDefault("night-end", "06:00"));
    }

    /** Reads "--name value" pairs, for example "--timezone Asia/Singapore". */
    public static BatchConfig fromArgs(String[] argv, Map<String, String> env) {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            if (!argv[i].startsWith("--") || i + 1 >= argv.length) {
                throw new IllegalArgumentException("Expected --name value pairs, got: " + argv[i]);
            }
            a.put(argv[i].substring(2), argv[++i]);
        }
        return new BatchConfig(a, env);
    }

    /** Spark settings for the two catalogs: lake (Iceberg) and clickhouse. */
    public Map<String, String> sparkConf() {
        Map<String, String> c = new LinkedHashMap<>();
        // Local days and hours in the stats use this time zone.
        c.put("spark.sql.session.timeZone", zone.getId());
        c.put("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

        String lake = "spark.sql.catalog.lake";
        c.put(lake, "org.apache.iceberg.spark.SparkCatalog");
        c.put(lake + ".type", "jdbc");
        c.put(lake + ".uri", require(args, "catalog-uri"));
        c.put(lake + ".jdbc.user", args.getOrDefault("catalog-user", "iceberg"));
        c.put(lake + ".jdbc.password", require(env, "POSTGRES_ICEBERG_PASSWORD"));
        c.put(lake + ".warehouse", require(args, "warehouse"));
        c.put(lake + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        c.put(lake + ".s3.endpoint", require(args, "s3-endpoint"));
        c.put(lake + ".s3.path-style-access", "true");
        c.put(lake + ".s3.access-key-id", require(env, "RUSTFS_ACCESS_KEY"));
        c.put(lake + ".s3.secret-access-key", require(env, "RUSTFS_SECRET_KEY"));
        // RustFS ignores the region, but the AWS SDK needs a value.
        c.put(lake + ".client.region", "us-east-1");

        String ch = "spark.sql.catalog.clickhouse";
        c.put(ch, "com.clickhouse.spark.ClickHouseCatalog");
        c.put(ch + ".host", require(args, "clickhouse-host"));
        c.put(ch + ".protocol", "http");
        c.put(ch + ".http_port", "8123");
        c.put(ch + ".user", "spark_writer");
        c.put(ch + ".password", require(env, "CLICKHOUSE_SPARK_PASSWORD"));
        c.put(ch + ".database", "hsec");
        return c;
    }

    private static String require(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing setting: " + key);
        }
        return v;
    }
}

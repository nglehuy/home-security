package hsec.stream;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/** Settings of the job. Terraform passes them as program arguments, and the passwords come from environment variables. */
public final class JobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public final ZoneId zone;
    /** Base URL of the Frigate UI, for links in alerts. It needs a login. */
    public final String frigateUrl;
    /** Base URL of the Frigate API on port 5000, for downloads. */
    public final String frigateApi;
    public final String kafkaBootstrap;
    public final Map<String, String> catalogProperties;

    JobConfig(ZoneId zone, String frigateUrl, String frigateApi, String kafkaBootstrap, Map<String, String> catalogProperties) {
        this.zone = zone;
        this.frigateUrl = stripSlash(frigateUrl);
        this.frigateApi = stripSlash(frigateApi);
        this.kafkaBootstrap = kafkaBootstrap;
        this.catalogProperties = Map.copyOf(catalogProperties);
    }

    /** Reads "--name value" pairs, for example "--timezone Asia/Singapore". */
    public static JobConfig fromArgs(String[] args, Map<String, String> env) {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("Expected --name value pairs, got: " + args[i]);
            }
            a.put(args[i].substring(2), args[++i]);
        }

        Map<String, String> catalog = new HashMap<>();
        catalog.put("uri", require(a, "catalog-uri"));
        catalog.put("jdbc.user", a.getOrDefault("catalog-user", "iceberg"));
        catalog.put("jdbc.password", require(env, "POSTGRES_ICEBERG_PASSWORD"));
        catalog.put("warehouse", require(a, "warehouse"));
        catalog.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalog.put("s3.endpoint", require(a, "s3-endpoint"));
        catalog.put("s3.path-style-access", "true");
        catalog.put("s3.access-key-id", require(env, "RUSTFS_ACCESS_KEY"));
        catalog.put("s3.secret-access-key", require(env, "RUSTFS_SECRET_KEY"));
        // RustFS ignores the region, but the AWS SDK needs a value.
        catalog.put("client.region", "us-east-1");

        return new JobConfig(
                ZoneId.of(require(a, "timezone")),
                require(a, "frigate-url"),
                require(a, "frigate-api"),
                require(a, "kafka-bootstrap"),
                catalog);
    }

    private static String require(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing setting: " + key);
        }
        return v;
    }

    private static String stripSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}

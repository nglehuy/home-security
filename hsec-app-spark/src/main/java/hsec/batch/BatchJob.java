package hsec.batch;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs every 15 minutes. Reads the Iceberg tables from local midnight of
 * yesterday, computes the stats, and appends them to ClickHouse. The first run
 * at or after 03:00 local time first does the nightly maintenance.
 */
public final class BatchJob {
    private static final Logger LOG = LoggerFactory.getLogger(BatchJob.class);

    private final SparkSession spark;
    private final BatchConfig config;

    BatchJob(SparkSession spark, BatchConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public static void main(String[] args) {
        BatchConfig config = BatchConfig.fromArgs(args, System.getenv());
        SparkSession.Builder b = SparkSession.builder().appName("hsec-batch");
        config.sparkConf().forEach(b::config);
        SparkSession spark = b.getOrCreate();
        try {
            new BatchJob(spark, config).run(ZonedDateTime.now(config.zone));
        } finally {
            spark.stop();
        }
    }

    void run(ZonedDateTime runStart) {
        if (Schedule.maintenanceDue(runStart)) {
            for (String sql : Maintenance.statements(runStart.toInstant())) {
                LOG.info("Maintenance: {}", sql);
                spark.sql(sql);
            }
        }

        Timestamp version = Timestamp.from(runStart.toInstant());
        Instant from = Schedule.windowStart(runStart);
        LOG.info("Window starts at {}", from);

        Dataset<Row> events = read("events", from, "event_id", "msg_type", "camera", "label", "sub_label",
                "top_score", "entered_zones", "face_score", "start_ts", "end_ts", "event_ts");
        Dataset<Row> visitRows = read("visits", from, "visit_id", "camera", "start_ts", "end_ts", "person_count",
                "known_names", "unknown_count", "zones");
        // Never read the image and clip columns.
        Dataset<Row> alertRows = read("alerts", from, "alert_id", "kind", "review_id", "camera", "severity",
                "objects", "sub_labels", "zones", "review_start", "event_id", "fired_ts", "snapshot_url");

        Dataset<Row> objects = Transforms.objects(events, version).cache();
        Dataset<Row> visits = Transforms.visits(visitRows, version).cache();
        Dataset<Row> alerts = Transforms.alerts(alertRows, version).cache();
        try {
            write("objects", objects);
            write("visits", visits);
            write("alerts", alerts);
            write("hourly_activity", Transforms.hourlyActivity(objects, version));
            write("daily_stats", Transforms.dailyStats(objects, visits, alerts, config.nightStart, config.nightEnd,
                    version));
        } finally {
            objects.unpersist();
            visits.unpersist();
            alerts.unpersist();
        }
    }

    private Dataset<Row> read(String table, Instant from, String... columns) {
        String first = columns[0];
        String[] rest = java.util.Arrays.copyOfRange(columns, 1, columns.length);
        return spark.table("lake.hsec." + table)
                .where(col("event_ts").geq(lit(Timestamp.from(from))))
                .select(first, rest);
    }

    private static void write(String table, Dataset<Row> rows) {
        try {
            rows.writeTo("clickhouse.hsec." + table).append();
        } catch (Exception e) {
            throw new IllegalStateException("Could not write clickhouse.hsec." + table, e);
        }
    }
}

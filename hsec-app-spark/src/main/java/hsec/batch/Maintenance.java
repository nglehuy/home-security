package hsec.batch;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Nightly upkeep of the Iceberg tables: retention, compaction, snapshot expiry,
 * orphan file removal, and manifest rewrite. Spark 4.0 runs these procedures
 * without the SQL extensions, but their names are case-sensitive.
 */
public final class Maintenance {
    public static final List<String> TABLES =
            List.of("events", "reviews", "object_updates", "triggers", "system_messages", "visits", "alerts");
    /** Snapshots stay for 1 day, so a restarted Flink job still finds its last commit. */
    public static final Duration SNAPSHOT_AGE = Duration.ofDays(1);

    private Maintenance() {}

    /** The SQL statements for one night, in order, for every table. */
    public static List<String> statements(Instant now) {
        String cutoff = sqlTimestamp(Schedule.retentionCutoff(now));
        String older = sqlTimestamp(now.minus(SNAPSHOT_AGE));
        List<String> out = new ArrayList<>();
        for (String t : TABLES) {
            String name = "hsec." + t;
            // The filter matches whole day partitions, so Iceberg deletes them without a rewrite.
            out.add("DELETE FROM lake." + name + " WHERE event_ts < " + cutoff);
            // alerts has few files per day, and a rewrite loads its clips into the 1 GiB executor.
            if (!"alerts".equals(t)) {
                out.add("CALL lake.system.rewrite_data_files(table => '" + name + "', strategy => 'sort')");
            }
            out.add("CALL lake.system.expire_snapshots(table => '" + name + "', older_than => " + older
                    + ", retain_last => 1)");
            out.add("CALL lake.system.remove_orphan_files(table => '" + name + "', older_than => " + older + ")");
            out.add("CALL lake.system.rewrite_manifests('" + name + "')");
        }
        return out;
    }

    private static String sqlTimestamp(Instant i) {
        return "TIMESTAMP '" + i.toString().replace('T', ' ').replace("Z", "") + " UTC'";
    }
}

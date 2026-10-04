package hsec.batch;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_sort;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.bool_or;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_trunc;
import static org.apache.spark.sql.functions.explode_outer;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.max_by;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.minute;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.unix_micros;
import static org.apache.spark.sql.functions.when;

import java.sql.Timestamp;
import java.time.LocalTime;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Computes the five ClickHouse tables from the Iceberg tables. Each result has the
 * columns of its ClickHouse table. Local days and hours use the session time zone.
 */
public final class Transforms {
    private Transforms() {}

    private static Column nonEmpty(Column c) {
        return c.isNotNull().and(c.notEqual(lit("")));
    }

    /**
     * One row per tracked object. known_name is the newest non-empty sub_label, and
     * end_ts comes from the end message.
     */
    public static Dataset<Row> objects(Dataset<Row> events, Timestamp version) {
        Column start = min("start_ts");
        Column end = max(when(col("msg_type").equalTo("end"), col("end_ts")));
        return events
                .groupBy("event_id")
                .agg(
                        max_by(col("camera"), col("event_ts")).as("camera"),
                        max_by(col("label"), col("event_ts")).as("label"),
                        // max_by skips rows whose ordering is null, so empty names never win.
                        max_by(col("sub_label"), when(nonEmpty(col("sub_label")), col("event_ts"))).as("known_name"),
                        coalesce(bool_or(col("face_score").isNotNull()), lit(false)).as("has_face"),
                        max("top_score").cast("float").as("top_score"),
                        array_sort(array_distinct(flatten(collect_list(coalesce(col("entered_zones"),
                                array().cast("array<string>")))))).as("zones"),
                        start.as("start_ts"),
                        end.as("end_ts"))
                .withColumn("duration_s",
                        ((unix_micros(col("end_ts")).minus(unix_micros(col("start_ts")))).divide(1e6)).cast("float"))
                .withColumn("version", lit(version))
                .select("event_id", "camera", "label", "known_name", "has_face", "top_score", "zones", "start_ts",
                        "end_ts", "duration_s", "version");
    }

    /** The closed visits from Iceberg, with their duration. */
    public static Dataset<Row> visits(Dataset<Row> visits, Timestamp version) {
        return visits
                .withColumn("duration_s",
                        ((unix_micros(col("end_ts")).minus(unix_micros(col("start_ts")))).divide(1e6)).cast("float"))
                .withColumn("version", lit(version))
                .select("visit_id", "camera", "start_ts", "end_ts", "duration_s", "person_count", "known_names",
                        "unknown_count", "zones", "version");
    }

    /** Message 1 of each alert, without the image and clip columns. */
    public static Dataset<Row> alerts(Dataset<Row> alerts, Timestamp version) {
        return alerts
                .where(col("kind").equalTo("start"))
                .withColumn("version", lit(version))
                .select("alert_id", "review_id", "camera", "severity", "objects", "sub_labels", "zones",
                        "review_start", "event_id", "fired_ts", "snapshot_url", "version");
    }

    /** Objects per local hour, camera, zone, and label. An object without a zone counts under the empty zone. */
    public static Dataset<Row> hourlyActivity(Dataset<Row> objects, Timestamp version) {
        Column person = col("label").equalTo("person");
        return objects
                .withColumn("zone", explode_outer(col("zones")))
                .withColumn("zone", coalesce(col("zone"), lit("")))
                .withColumn("hour", date_trunc("HOUR", col("start_ts")))
                .groupBy("hour", "camera", "zone", "label")
                .agg(
                        count(lit(1)).cast("int").as("objects"),
                        count(when(person.and(col("known_name").isNotNull()), 1)).cast("int").as("known_people"),
                        count(when(col("has_face").and(col("known_name").isNull()), 1)).cast("int")
                                .as("unknown_faces"),
                        avg("top_score").cast("float").as("avg_top_score"))
                .withColumn("version", lit(version))
                .select("hour", "camera", "zone", "label", "objects", "known_people", "unknown_faces",
                        "avg_top_score", "version");
    }

    /**
     * Stats per local day and camera. people, faces, and scores come from objects,
     * visits from the visits rows, and alerts from the alert rows.
     */
    public static Dataset<Row> dailyStats(Dataset<Row> objects, Dataset<Row> visits, Dataset<Row> alerts,
            LocalTime nightStart, LocalTime nightEnd, Timestamp version) {
        Column night = night(col("start_ts"), nightStart, nightEnd);
        Column face = col("has_face");
        Dataset<Row> fromObjects = objects
                .withColumn("day", to_date(col("start_ts")))
                .groupBy("day", "camera")
                .agg(
                        count(when(col("label").equalTo("person"), 1)).cast("int").as("people"),
                        count(when(face, 1)).cast("int").as("faces_seen"),
                        count(when(face.and(col("known_name").isNull()), 1)).cast("int").as("unknown_faces"),
                        avg(when(not(night), col("top_score"))).cast("float").as("avg_score_day"),
                        avg(when(night, col("top_score"))).cast("float").as("avg_score_night"));
        Dataset<Row> fromVisits = visits
                .withColumn("day", to_date(col("start_ts")))
                .groupBy("day", "camera")
                .agg(count(lit(1)).cast("int").as("visits"));
        Dataset<Row> fromAlerts = alerts
                .withColumn("day", to_date(col("fired_ts")))
                .groupBy("day", "camera")
                .agg(count(lit(1)).cast("int").as("alerts"));

        return fromObjects
                .join(fromVisits, new String[] {"day", "camera"}, "full_outer")
                .join(fromAlerts, new String[] {"day", "camera"}, "full_outer")
                .withColumn("visits", coalesce(col("visits"), lit(0)))
                .withColumn("people", coalesce(col("people"), lit(0)))
                .withColumn("faces_seen", coalesce(col("faces_seen"), lit(0)))
                .withColumn("unknown_faces", coalesce(col("unknown_faces"), lit(0)))
                .withColumn("alerts", coalesce(col("alerts"), lit(0)))
                .withColumn("unknown_face_rate", when(col("faces_seen").gt(0),
                        col("unknown_faces").divide(col("faces_seen"))).otherwise(lit(0)).cast("float"))
                .withColumn("version", lit(version))
                .select("day", "camera", "visits", "people", "faces_seen", "unknown_faces", "unknown_face_rate",
                        "avg_score_day", "avg_score_night", "alerts", "version");
    }

    /** The night test of Schedule.isNight as a Spark column, on the local time of ts. */
    static Column night(Column ts, LocalTime start, LocalTime end) {
        Column m = hour(ts).multiply(60).plus(minute(ts));
        int s = start.getHour() * 60 + start.getMinute();
        int e = end.getHour() * 60 + end.getMinute();
        if (s < e) {
            return m.geq(s).and(m.lt(e));
        }
        return m.geq(s).or(m.lt(e));
    }
}

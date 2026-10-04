package hsec.stream;

import java.util.List;
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * The seven Iceberg tables. The migrations in hsec-db-iceberg/migrations create them,
 * and the job never creates or changes a table. The tests use these schemas to make
 * test tables, and MigrationSchemaTest makes sure that they match the migrations.
 */
public final class Tables {
    public static final String NAMESPACE = "hsec";

    public static final String EVENTS = "events";
    public static final String REVIEWS = "reviews";
    public static final String OBJECT_UPDATES = "object_updates";
    public static final String TRIGGERS = "triggers";
    public static final String SYSTEM_MESSAGES = "system_messages";
    public static final String VISITS = "visits";
    public static final String ALERTS = "alerts";

    /** Table properties that every migration sets explicitly. */
    public static final Map<String, String> PROPERTIES = Map.of(
            "format-version", "2",
            "write.format.default", "parquet",
            "write.parquet.compression-codec", "zstd",
            "write.delete.mode", "copy-on-write");

    private static final Type STR = Types.StringType.get();
    private static final Type DBL = Types.DoubleType.get();
    private static final Type INT = Types.IntegerType.get();
    private static final Type BOOL = Types.BooleanType.get();
    private static final Type TS = Types.TimestampType.withZone();
    private static final Type BIN = Types.BinaryType.get();

    private Tables() {}

    public static final Map<String, Schema> SCHEMAS = Map.of(
            EVENTS, schema(
                    col("message_id", STR), col("event_id", STR), col("msg_type", STR), col("camera", STR),
                    col("label", STR), col("sub_label", STR), col("sub_label_score", DBL), col("score", DBL),
                    col("top_score", DBL), list("current_zones"), list("entered_zones"), col("face_score", DBL),
                    col("has_snapshot", BOOL), col("has_clip", BOOL), col("start_ts", TS), col("end_ts", TS),
                    col("event_ts", TS), col("ingest_ts", TS)),
            REVIEWS, schema(
                    col("message_id", STR), col("review_id", STR), col("msg_type", STR), col("camera", STR),
                    col("severity", STR), col("start_ts", TS), col("end_ts", TS), list("detections"),
                    list("objects"), list("sub_labels"), list("zones"), col("event_ts", TS), col("ingest_ts", TS)),
            OBJECT_UPDATES, schema(
                    col("message_id", STR), col("event_id", STR), col("update_type", STR), col("camera", STR),
                    col("name", STR), col("score", DBL), col("plate", STR), col("description", STR),
                    col("model", STR), col("sub_label", STR), col("attribute", STR), col("event_ts", TS),
                    col("ingest_ts", TS)),
            TRIGGERS, schema(
                    col("message_id", STR), col("event_id", STR), col("camera", STR), col("name", STR),
                    col("type", STR), col("score", DBL), col("event_ts", TS), col("ingest_ts", TS)),
            SYSTEM_MESSAGES, schema(
                    col("message_id", STR), col("topic", STR), col("payload", STR), col("event_ts", TS),
                    col("ingest_ts", TS)),
            VISITS, schema(
                    col("visit_id", STR), col("camera", STR), col("start_ts", TS), col("end_ts", TS),
                    col("person_count", INT), list("known_names"), col("unknown_count", INT), list("zones"),
                    col("event_ts", TS), col("ingest_ts", TS)),
            ALERTS, schema(
                    col("alert_id", STR), col("kind", STR), col("review_id", STR), col("camera", STR),
                    col("severity", STR), list("objects"), list("sub_labels"), list("zones"),
                    col("review_start", TS), col("event_id", STR), col("fired_ts", TS), col("snapshot_url", STR),
                    col("part", INT), col("part_count", INT), col("clip_url", STR), col("video_camera", STR),
                    col("video_from_ts", TS), col("video_to_ts", TS), col("image", BIN), col("clip", BIN),
                    col("event_ts", TS), col("ingest_ts", TS)));

    public static Schema schema(String table) {
        return SCHEMAS.get(table);
    }

    /** One partition per UTC day of event_ts. */
    public static PartitionSpec spec(Schema schema) {
        return PartitionSpec.builderFor(schema).day("event_ts").build();
    }

    /** Camera, then event_ts. system_messages has no camera, so it sorts by topic. */
    public static SortOrder sortOrder(String table, Schema schema) {
        String first = SYSTEM_MESSAGES.equals(table) ? "topic" : "camera";
        return SortOrder.builderFor(schema).asc(first).asc("event_ts").build();
    }

    // Field IDs only need to be unique here. Iceberg assigns new IDs when it creates a table.
    private static int nextId;

    private static Types.NestedField col(String name, Type type) {
        return optional(++nextId, name, type);
    }

    private static Types.NestedField list(String name) {
        int listId = ++nextId;
        return optional(listId, name, Types.ListType.ofOptional(++nextId, STR));
    }

    private static Schema schema(Types.NestedField... fields) {
        return new Schema(List.of(fields));
    }
}

package hsec.batch;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import org.junit.jupiter.api.Test;

class TransformsTest extends SparkTestBase {
    static final StructType EVENTS = new StructType()
            .add("event_id", DataTypes.StringType).add("msg_type", DataTypes.StringType)
            .add("camera", DataTypes.StringType).add("label", DataTypes.StringType)
            .add("sub_label", DataTypes.StringType).add("top_score", DataTypes.DoubleType)
            .add("entered_zones", DataTypes.createArrayType(DataTypes.StringType))
            .add("face_score", DataTypes.DoubleType).add("start_ts", DataTypes.TimestampType)
            .add("end_ts", DataTypes.TimestampType).add("event_ts", DataTypes.TimestampType);
    static final StructType VISITS = new StructType()
            .add("visit_id", DataTypes.StringType).add("camera", DataTypes.StringType)
            .add("start_ts", DataTypes.TimestampType).add("end_ts", DataTypes.TimestampType)
            .add("person_count", DataTypes.IntegerType)
            .add("known_names", DataTypes.createArrayType(DataTypes.StringType))
            .add("unknown_count", DataTypes.IntegerType)
            .add("zones", DataTypes.createArrayType(DataTypes.StringType));
    static final StructType ALERTS = new StructType()
            .add("alert_id", DataTypes.StringType).add("kind", DataTypes.StringType)
            .add("review_id", DataTypes.StringType).add("camera", DataTypes.StringType)
            .add("severity", DataTypes.StringType)
            .add("objects", DataTypes.createArrayType(DataTypes.StringType))
            .add("sub_labels", DataTypes.createArrayType(DataTypes.StringType))
            .add("zones", DataTypes.createArrayType(DataTypes.StringType))
            .add("review_start", DataTypes.TimestampType).add("event_id", DataTypes.StringType)
            .add("fired_ts", DataTypes.TimestampType).add("snapshot_url", DataTypes.StringType);

    static Row event(String id, String type, String camera, String label, String name, double score,
            List<String> zones, Double face, String start, String end, String at) {
        return RowFactory.create(id, type, camera, label, name, score, zones, face, ts(start),
                end == null ? null : ts(end), ts(at));
    }

    static Dataset<Row> events(Row... rows) {
        return spark.createDataFrame(Arrays.asList(rows), EVENTS);
    }

    static Dataset<Row> visits(Row... rows) {
        return spark.createDataFrame(Arrays.asList(rows), VISITS);
    }

    static Dataset<Row> alerts(Row... rows) {
        return spark.createDataFrame(Arrays.asList(rows), ALERTS);
    }

    static Map<String, Row> byId(Dataset<Row> objects) {
        return objects.collectAsList().stream().collect(Collectors.toMap(r -> r.getString(0), r -> r));
    }

    @Test
    void newestNonEmptyNameWins() {
        Dataset<Row> o = Transforms.objects(events(
                event("p1", "new", "front", "person", null, 0.5, List.of(), null, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:00"),
                event("p1", "update", "front", "person", "Ann", 0.6, List.of(), null, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:05"),
                event("p1", "update", "front", "person", "Bob", 0.7, List.of(), null, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:10"),
                event("p1", "end", "front", "person", "", 0.8, List.of(), null, "2026-10-04 10:00:00",
                        "2026-10-04 10:00:30", "2026-10-04 10:00:30")), VERSION);
        Row r = byId(o).get("p1");
        assertThat(r.<String>getAs("known_name")).isEqualTo("Bob");
        assertThat(r.<Float>getAs("top_score")).isEqualTo(0.8f);
        assertThat(r.<Timestamp>getAs("end_ts")).isEqualTo(ts("2026-10-04 10:00:30"));
        assertThat(r.<Float>getAs("duration_s")).isEqualTo(30f);
        assertThat(r.<Timestamp>getAs("version")).isEqualTo(VERSION);
    }

    @Test
    void faceFlagIsSetByAnyFaceScore() {
        Dataset<Row> o = Transforms.objects(events(
                event("p1", "new", "front", "person", null, 0.5, List.of(), null, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:00"),
                event("p1", "update", "front", "person", null, 0.5, List.of(), 0.8, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:01"),
                event("p2", "new", "front", "person", null, 0.5, List.of(), null, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:00")), VERSION);
        assertThat(byId(o).get("p1").<Boolean>getAs("has_face")).isTrue();
        assertThat(byId(o).get("p2").<Boolean>getAs("has_face")).isFalse();
        // An object without an end message has no end time.
        assertThat(byId(o).get("p2").<Timestamp>getAs("end_ts")).isNull();
        assertThat(byId(o).get("p2").<Float>getAs("duration_s")).isNull();
    }

    @Test
    void zonesAreTheUnionOfEnteredZones() {
        Dataset<Row> o = Transforms.objects(events(
                event("c1", "new", "drive", "car", null, 0.5, List.of("gate"), null, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:00"),
                event("c1", "update", "drive", "car", null, 0.5, List.of("gate", "drive"), null,
                        "2026-10-04 10:00:00", null, "2026-10-04 10:00:01"),
                event("c1", "update", "drive", "car", null, 0.5, null, null, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:02")), VERSION);
        assertThat(byId(o).get("c1").<scala.collection.Seq<String>>getAs("zones"))
                .satisfies(z -> assertThat(scala.jdk.javaapi.CollectionConverters.asJava(z))
                        .containsExactly("drive", "gate"));
    }

    @Test
    void hourlyGroupsByLocalHourZoneAndLabel() {
        Dataset<Row> objects = Transforms.objects(events(
                event("p1", "new", "front", "person", "Ann", 0.8, List.of("yard"), 0.9, "2026-10-04 10:05:00", null,
                        "2026-10-04 10:05:00"),
                event("p2", "new", "front", "person", null, 0.6, List.of("yard", "porch"), 0.9,
                        "2026-10-04 10:55:00", null, "2026-10-04 10:55:00"),
                event("p3", "new", "front", "person", null, 0.4, List.of(), null, "2026-10-04 11:00:00", null,
                        "2026-10-04 11:00:00")), VERSION);
        List<Row> h = Transforms.hourlyActivity(objects, VERSION).orderBy("hour", "zone").collectAsList();
        assertThat(h).hasSize(3);
        Row yard = h.stream().filter(r -> r.getAs("zone").equals("yard")).findFirst().orElseThrow();
        assertThat(yard.<Timestamp>getAs("hour")).isEqualTo(ts("2026-10-04 10:00:00"));
        assertThat(yard.<Integer>getAs("objects")).isEqualTo(2);
        assertThat(yard.<Integer>getAs("known_people")).isEqualTo(1);
        assertThat(yard.<Integer>getAs("unknown_faces")).isEqualTo(1);
        assertThat(yard.<Float>getAs("avg_top_score")).isCloseTo(0.7f, within(1e-5f));
        Row noZone = h.stream().filter(r -> r.getAs("zone").equals("")).findFirst().orElseThrow();
        assertThat(noZone.<Timestamp>getAs("hour")).isEqualTo(ts("2026-10-04 11:00:00"));
    }

    @Test
    void nightWindowCrossesMidnight() {
        Dataset<Row> objects = Transforms.objects(events(
                event("a", "new", "front", "person", null, 0.2, List.of(), null, "2026-10-04 23:30:00", null,
                        "2026-10-04 23:30:00"),
                event("b", "new", "front", "person", null, 0.4, List.of(), null, "2026-10-05 05:59:00", null,
                        "2026-10-05 05:59:00"),
                event("c", "new", "front", "person", null, 0.9, List.of(), null, "2026-10-05 06:00:00", null,
                        "2026-10-05 06:00:00"),
                event("d", "new", "front", "person", null, 0.7, List.of(), null, "2026-10-05 22:59:00", null,
                        "2026-10-05 22:59:00")), VERSION);
        List<Row> d = Transforms.dailyStats(objects, visits(), alerts(), LocalTime.of(23, 0), LocalTime.of(6, 0),
                VERSION).orderBy("day").collectAsList();
        assertThat(d).hasSize(2);
        assertThat(d.get(0).<Date>getAs("day")).isEqualTo(Date.valueOf("2026-10-04"));
        assertThat(d.get(0).<Float>getAs("avg_score_night")).isEqualTo(0.2f);
        assertThat(d.get(0).<Float>getAs("avg_score_day")).isNull();
        assertThat(d.get(1).<Float>getAs("avg_score_night")).isEqualTo(0.4f);
        assertThat(d.get(1).<Float>getAs("avg_score_day")).isCloseTo(0.8f, within(1e-5f));
    }

    @Test
    void dailyStatsCountVisitsFacesAndAlerts() {
        Dataset<Row> objects = Transforms.objects(events(
                event("p1", "new", "front", "person", "Ann", 0.8, List.of(), 0.9, "2026-10-04 10:00:00", null,
                        "2026-10-04 10:00:00"),
                event("p2", "new", "front", "person", null, 0.6, List.of(), 0.9, "2026-10-04 11:00:00", null,
                        "2026-10-04 11:00:00"),
                event("c1", "new", "front", "car", null, 0.9, List.of(), null, "2026-10-04 12:00:00", null,
                        "2026-10-04 12:00:00")), VERSION);
        Dataset<Row> v = Transforms.visits(visits(
                RowFactory.create("front:1", "front", ts("2026-10-04 10:00:00"), ts("2026-10-04 10:10:00"), 1,
                        List.of("Ann"), 0, List.of())), VERSION);
        Dataset<Row> a = Transforms.alerts(alerts(
                RowFactory.create("r1:start", "start", "r1", "front", "alert", List.of("person"), List.of(),
                        List.of(), ts("2026-10-04 10:00:00"), "p1", ts("2026-10-04 10:00:01"), "u"),
                RowFactory.create("r1:clip:1", "clip", "r1", "front", "alert", List.of("person"), List.of(),
                        List.of(), ts("2026-10-04 10:00:00"), null, ts("2026-10-04 10:00:01"), null),
                RowFactory.create("r2:start", "start", "r2", "garage", "alert", List.of("car"), List.of(),
                        List.of(), ts("2026-10-04 12:00:00"), "c9", ts("2026-10-04 12:00:01"), "u")), VERSION);
        Map<String, Row> d = Transforms.dailyStats(objects, v, a, LocalTime.of(23, 0), LocalTime.of(6, 0), VERSION)
                .collectAsList().stream().collect(Collectors.toMap(r -> r.getAs("camera"), r -> r));

        Row front = d.get("front");
        assertThat(front.<Integer>getAs("visits")).isEqualTo(1);
        assertThat(front.<Integer>getAs("people")).isEqualTo(2);
        assertThat(front.<Integer>getAs("faces_seen")).isEqualTo(2);
        assertThat(front.<Integer>getAs("unknown_faces")).isEqualTo(1);
        assertThat(front.<Float>getAs("unknown_face_rate")).isEqualTo(0.5f);
        // Only message 1 counts as an alert.
        assertThat(front.<Integer>getAs("alerts")).isEqualTo(1);

        // A camera with only an alert still gets a row, with zeros elsewhere.
        Row garage = d.get("garage");
        assertThat(garage.<Integer>getAs("alerts")).isEqualTo(1);
        assertThat(garage.<Integer>getAs("people")).isZero();
        assertThat(garage.<Float>getAs("unknown_face_rate")).isZero();
    }

    @Test
    void alertsDropTheClipPartsAndTheFiles() {
        Dataset<Row> a = Transforms.alerts(alerts(
                RowFactory.create("r1:start", "start", "r1", "front", "alert", List.of("person"), List.of(),
                        List.of("yard"), ts("2026-10-04 10:00:00"), "p1", ts("2026-10-04 10:00:01"), "u"),
                RowFactory.create("r1:clip:1", "clip", "r1", "front", "alert", List.of(), List.of(), List.of(),
                        ts("2026-10-04 10:00:00"), null, ts("2026-10-04 10:00:01"), null)), VERSION);
        assertThat(a.collectAsList()).extracting(r -> r.getString(0)).containsExactly("r1:start");
        assertThat(a.columns()).doesNotContain("image", "clip", "kind");
    }

    @Test
    void emptyInputGivesEmptyTables() {
        Dataset<Row> objects = Transforms.objects(events(), VERSION);
        assertThat(objects.count()).isZero();
        assertThat(Transforms.hourlyActivity(objects, VERSION).count()).isZero();
        assertThat(Transforms.dailyStats(objects, Transforms.visits(visits(), VERSION),
                Transforms.alerts(alerts(), VERSION), LocalTime.of(23, 0), LocalTime.of(6, 0), VERSION).count())
                .isZero();
    }
}

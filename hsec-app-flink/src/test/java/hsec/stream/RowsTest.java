package hsec.stream;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class RowsTest {
    @Test
    void keepsMicroseconds() {
        TimestampData t = Times.fromSeconds(1607123955.475377);
        assertThat(t.getMillisecond()).isEqualTo(1607123955475L);
        assertThat(t.getNanoOfMillisecond()).isEqualTo(377_000);
        assertThat(Times.fromSeconds(null)).isNull();
    }

    @Test
    void formatsSecondsWithSixDecimals() {
        assertThat(Times.format(1700000000)).isEqualTo("1700000000.000000");
        assertThat(Times.format(1718987154.308396)).isEqualTo("1718987154.308396");
    }

    @Test
    void eventRowFollowsTheSchema() {
        Envelope e = Parser.parse(Samples.raw("frigate.events", Samples.EVENT_NEW), 1_700_000_000_000L).orElseThrow();
        RowData r = Rows.event(e);
        assertThat(r.getArity()).isEqualTo(Tables.schema(Tables.EVENTS).columns().size());
        assertThat(r.getString(1).toString()).isEqualTo("1607123955.475377-mxklsc");
        assertThat(r.getArray(10).size()).isEqualTo(2);
        assertThat(r.isNullAt(15)).isTrue();
        // event_ts is the frame time.
        assertThat(r.getTimestamp(16, 6).getMillisecond()).isEqualTo(1607123961837L);
        assertThat(r.getTimestamp(17, 6).getMillisecond()).isEqualTo(1_700_000_000_000L);
    }

    @Test
    void systemRowUsesTheIngestTime() {
        Envelope e = Parser.parse(Samples.raw("frigate.available", "online"), 1234L).orElseThrow();
        RowData r = Rows.system(e);
        assertThat(r.getArity()).isEqualTo(Tables.schema(Tables.SYSTEM_MESSAGES).columns().size());
        assertThat(r.getString(1).toString()).isEqualTo("frigate.available");
        assertThat(r.getTimestamp(3, 6).getMillisecond()).isEqualTo(1234L);
    }

    @Test
    void everyRowHasTheArityOfItsTable() {
        Envelope review = Parser.parse(Samples.raw("frigate.reviews", Samples.REVIEW_NEW), 1).orElseThrow();
        Envelope update = Parser.parse(Samples.raw("frigate.tracked_object_update", Samples.DESCRIPTION_UPDATE), 1)
                .orElseThrow();
        Envelope trigger = Parser.parse(Samples.raw("frigate.triggers", Samples.TRIGGER), 1).orElseThrow();
        assertThat(Rows.review(review).getArity()).isEqualTo(Tables.schema(Tables.REVIEWS).columns().size());
        assertThat(Rows.objectUpdate(update).getArity()).isEqualTo(Tables.schema(Tables.OBJECT_UPDATES).columns().size());
        // A description update has no time, so event_ts falls back to the ingest time.
        assertThat(Rows.objectUpdate(update).getTimestamp(11, 6).getMillisecond()).isEqualTo(1L);
        assertThat(Rows.trigger(trigger).getArity()).isEqualTo(Tables.schema(Tables.TRIGGERS).columns().size());

        Visit v = new Visit();
        v.visitId = "c:1";
        v.camera = "c";
        v.startTime = 1.0;
        v.endTime = 2.0;
        assertThat(Rows.visit(v).getArity()).isEqualTo(Tables.schema(Tables.VISITS).columns().size());
        assertThat(Rows.visit(v).getArray(5).size()).isZero();
    }
}

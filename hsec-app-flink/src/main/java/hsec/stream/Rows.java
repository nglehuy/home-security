package hsec.stream;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/** Builds Iceberg rows. The field order follows the schemas in Tables. */
public final class Rows {
    private Rows() {}

    public static RowData event(Envelope e) {
        EventMsg m = e.event;
        Double eventTime = m.frameTime != null ? m.frameTime : m.startTime;
        return GenericRowData.of(
                str(e.messageId), str(m.id), str(m.type), str(m.camera), str(m.label), str(m.subLabel),
                m.subLabelScore, m.score, m.topScore, arr(m.currentZones), arr(m.enteredZones), m.faceScore,
                m.hasSnapshot, m.hasClip, Times.fromSeconds(m.startTime), Times.fromSeconds(m.endTime),
                eventTime != null ? Times.fromSeconds(eventTime) : Times.fromMillis(e.ingestMs),
                Times.fromMillis(e.ingestMs));
    }

    public static RowData review(Envelope e) {
        ReviewMsg m = e.review;
        return GenericRowData.of(
                str(e.messageId), str(m.id), str(m.type), str(m.camera), str(m.severity),
                Times.fromSeconds(m.startTime), Times.fromSeconds(m.endTime), arr(m.detections), arr(m.objects),
                arr(m.subLabels), arr(m.zones),
                m.startTime != null ? Times.fromSeconds(m.startTime) : Times.fromMillis(e.ingestMs),
                Times.fromMillis(e.ingestMs));
    }

    public static RowData objectUpdate(Envelope e) {
        ObjectUpdateMsg m = e.update;
        return GenericRowData.of(
                str(e.messageId), str(m.id), str(m.type), str(m.camera), str(m.name), m.score, str(m.plate),
                str(m.description), str(m.model), str(m.subLabel), str(m.attribute),
                m.timestamp != null ? Times.fromSeconds(m.timestamp) : Times.fromMillis(e.ingestMs),
                Times.fromMillis(e.ingestMs));
    }

    public static RowData trigger(Envelope e) {
        TriggerMsg m = e.trigger;
        return GenericRowData.of(
                str(e.messageId), str(m.eventId), str(m.camera), str(m.name), str(m.type), m.score,
                m.receiveTime != null ? Times.fromSeconds(m.receiveTime) : Times.fromMillis(e.ingestMs),
                Times.fromMillis(e.ingestMs));
    }

    /** System messages carry no time of their own, so event_ts is the ingest time. */
    public static RowData system(Envelope e) {
        return GenericRowData.of(
                str(e.messageId), str(e.topic), str(e.system.payload),
                Times.fromMillis(e.ingestMs), Times.fromMillis(e.ingestMs));
    }

    public static RowData visit(Visit v) {
        return GenericRowData.of(
                str(v.visitId), str(v.camera), Times.fromSeconds(v.startTime), Times.fromSeconds(v.endTime),
                v.personCount, arr(v.knownNames), v.unknownCount, arr(v.zones),
                Times.fromSeconds(v.startTime), Times.fromMillis(v.ingestMs));
    }

    /** The video field becomes three columns, and the files become binary columns. */
    public static RowData alert(AlertMessage a) {
        return GenericRowData.of(
                str(a.alertId), str(a.kind), str(a.reviewId), str(a.camera), str(a.severity), arr(a.objects),
                arr(a.subLabels), arr(a.zones), Times.fromSeconds(a.reviewStart), str(a.eventId),
                Times.fromMillis(a.firedMs), str(a.snapshotUrl), a.part, a.partCount, str(a.clipUrl),
                str(a.camera), Times.fromSeconds(a.videoFrom), Times.fromSeconds(a.videoTo),
                a.imageJpeg, a.clipMp4, Times.fromSeconds(a.videoFrom), Times.fromMillis(a.ingestMs));
    }

    private static StringData str(String s) {
        return s == null ? null : StringData.fromString(s);
    }

    private static ArrayData arr(String[] values) {
        if (values == null) {
            return new GenericArrayData(new Object[0]);
        }
        Object[] out = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = str(values[i]);
        }
        return new GenericArrayData(out);
    }
}

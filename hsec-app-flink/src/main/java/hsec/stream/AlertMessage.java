package hsec.stream;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** One message on hsec.alerts: message 1 with the image (kind start), or one clip part (kind clip). */
public class AlertMessage {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter FIRED = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    public String alertId;
    public String kind;
    public String reviewId;
    public String camera;
    public String severity;
    public String[] objects;
    public String[] subLabels;
    public String[] zones;
    public double reviewStart;
    public String eventId;
    public long firedMs;
    /** fired_ts in the local time zone, for example 2026-10-04T21:05:31.120+08:00. */
    public String firedTs;
    public String snapshotUrl;
    public Integer part;
    public Integer partCount;
    public String clipUrl;
    public double videoFrom;
    public double videoTo;
    public byte[] imageJpeg;
    public byte[] clipMp4;
    public long ingestMs;

    public AlertMessage() {}

    /** Builds the message for a request. The file is null if the download failed. */
    public static AlertMessage from(AlertRequest r, byte[] file, String frigateUrl, ZoneId zone, long nowMs) {
        AlertMessage m = new AlertMessage();
        m.alertId = r.alertId;
        m.kind = r.kind;
        m.reviewId = r.reviewId;
        m.camera = r.camera;
        m.severity = r.severity;
        m.objects = r.objects;
        m.subLabels = r.subLabels;
        m.zones = r.zones;
        m.reviewStart = r.reviewStart;
        m.eventId = r.eventId;
        m.firedMs = r.firedMs;
        m.firedTs = FIRED.format(Instant.ofEpochMilli(r.firedMs).atZone(zone));
        m.videoFrom = r.from;
        m.videoTo = r.to;
        m.ingestMs = nowMs;
        if (AlertRequest.CLIP.equals(r.kind)) {
            m.part = r.part;
            m.partCount = r.partCount;
            m.clipUrl = frigateUrl + "/api/" + r.camera + "/start/" + Times.format(r.from)
                    + "/end/" + Times.format(r.to) + "/clip.mp4";
            m.clipMp4 = file;
        } else {
            m.snapshotUrl = r.eventId == null ? null : frigateUrl + "/api/events/" + r.eventId + "/snapshot.jpg";
            m.imageJpeg = file;
        }
        return m;
    }

    /** The JSON body on hsec.alerts. Files are in base64. */
    public byte[] toJson() {
        ObjectNode n = JSON.createObjectNode();
        n.put("alert_id", alertId);
        n.put("kind", kind);
        n.put("review_id", reviewId);
        n.put("camera", camera);
        if (AlertRequest.START.equals(kind)) {
            n.put("severity", severity);
            strings(n.putArray("objects"), objects);
            strings(n.putArray("sub_labels"), subLabels);
            strings(n.putArray("zones"), zones);
        }
        n.put("review_start", plain(reviewStart));
        if (AlertRequest.START.equals(kind)) {
            n.put("event_id", eventId);
            n.put("fired_ts", firedTs);
        } else {
            n.put("part", part);
            n.put("part_count", partCount);
        }
        ObjectNode v = n.putObject("video");
        v.put("camera", camera);
        v.put("from", plain(videoFrom));
        v.put("to", plain(videoTo));
        if (AlertRequest.START.equals(kind)) {
            n.put("snapshot_url", snapshotUrl);
            n.put("image_jpeg", imageJpeg == null ? null : Base64.getEncoder().encodeToString(imageJpeg));
        } else {
            n.put("clip_url", clipUrl);
            n.put("clip_mp4", clipMp4 == null ? null : Base64.getEncoder().encodeToString(clipMp4));
        }
        try {
            return JSON.writeValueAsString(n).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Unix seconds as a plain decimal with 6 places. A double would print as 1.7E9. */
    private static BigDecimal plain(double seconds) {
        return new BigDecimal(Times.format(seconds));
    }

    private static void strings(ArrayNode a, String[] values) {
        if (values != null) {
            for (String s : values) {
                a.add(s);
            }
        }
    }
}

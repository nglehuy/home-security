package hsec.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/** Turns raw Frigate messages into typed messages. It never throws on bad input. */
public final class Parser {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Parser() {}

    /**
     * Parses one record. A system record keeps its raw text. A camera record that
     * does not parse, or misses its ID or camera, gives an empty result.
     */
    public static Optional<Envelope> parse(RawRecord r, long ingestMs) {
        Envelope e = new Envelope();
        e.topic = r.topic;
        e.messageId = messageId(r.topic, r.value);
        e.ingestMs = ingestMs;

        switch (r.topic) {
            case "frigate.events" -> {
                JsonNode after = json(r.value).map(n -> n.path("after")).orElse(null);
                if (after == null || text(after, "id") == null || text(after, "camera") == null) {
                    return Optional.empty();
                }
                e.kind = Envelope.EVENT;
                e.event = event(json(r.value).get().path("type").asText(null), after);
            }
            case "frigate.reviews" -> {
                JsonNode after = json(r.value).map(n -> n.path("after")).orElse(null);
                if (after == null || text(after, "id") == null || text(after, "camera") == null) {
                    return Optional.empty();
                }
                e.kind = Envelope.REVIEW;
                e.review = review(json(r.value).get().path("type").asText(null), after);
            }
            case "frigate.tracked_object_update" -> {
                JsonNode n = json(r.value).orElse(null);
                if (n == null || text(n, "id") == null || text(n, "type") == null) {
                    return Optional.empty();
                }
                e.kind = Envelope.OBJECT_UPDATE;
                e.update = update(n);
            }
            case "frigate.triggers" -> {
                JsonNode n = json(r.value).orElse(null);
                if (n == null || text(n, "event_id") == null || text(n, "camera") == null) {
                    return Optional.empty();
                }
                e.kind = Envelope.TRIGGER;
                TriggerMsg t = new TriggerMsg();
                t.eventId = text(n, "event_id");
                t.camera = text(n, "camera");
                t.name = text(n, "name");
                t.type = text(n, "type");
                t.score = number(n, "score");
                t.receiveTime = r.videoFrom;
                e.trigger = t;
            }
            default -> {
                e.kind = Envelope.SYSTEM;
                e.system = new SystemMsg(new String(r.value, StandardCharsets.UTF_8));
            }
        }
        return Optional.of(e);
    }

    /** SHA-256 of the topic, a zero byte, and the raw bytes, in hex. */
    public static String messageId(String topic, byte[] value) {
        try {
            MessageDigest d = MessageDigest.getInstance("SHA-256");
            d.update(topic.getBytes(StandardCharsets.UTF_8));
            d.update((byte) 0);
            d.update(value);
            return HexFormat.of().formatHex(d.digest());
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }

    static EventMsg event(String type, JsonNode a) {
        EventMsg m = new EventMsg();
        m.type = type;
        m.id = text(a, "id");
        m.camera = text(a, "camera");
        m.label = text(a, "label");
        // Frigate sends sub_label as [name, score], as a plain name, or as null.
        JsonNode sub = a.path("sub_label");
        if (sub.isArray() && sub.size() > 0) {
            m.subLabel = sub.get(0).isNull() ? null : sub.get(0).asText();
            m.subLabelScore = sub.size() > 1 && sub.get(1).isNumber() ? sub.get(1).asDouble() : null;
        } else if (sub.isTextual()) {
            m.subLabel = sub.asText();
        }
        m.score = number(a, "score");
        m.topScore = number(a, "top_score");
        m.currentZones = strings(a.path("current_zones"));
        m.enteredZones = strings(a.path("entered_zones"));
        m.faceScore = number(a.path("attributes"), "face");
        m.hasSnapshot = bool(a, "has_snapshot");
        m.hasClip = bool(a, "has_clip");
        m.startTime = number(a, "start_time");
        m.endTime = number(a, "end_time");
        m.frameTime = number(a, "frame_time");
        return m;
    }

    static ReviewMsg review(String type, JsonNode a) {
        ReviewMsg m = new ReviewMsg();
        m.type = type;
        m.id = text(a, "id");
        m.camera = text(a, "camera");
        m.severity = text(a, "severity");
        m.startTime = number(a, "start_time");
        m.endTime = number(a, "end_time");
        JsonNode data = a.path("data");
        m.detections = strings(data.path("detections"));
        m.objects = strings(data.path("objects"));
        m.subLabels = strings(data.path("sub_labels"));
        m.zones = strings(data.path("zones"));
        return m;
    }

    static ObjectUpdateMsg update(JsonNode n) {
        ObjectUpdateMsg m = new ObjectUpdateMsg();
        m.type = text(n, "type");
        m.id = text(n, "id");
        m.camera = text(n, "camera");
        m.name = text(n, "name");
        m.score = number(n, "score");
        m.plate = text(n, "plate");
        m.description = text(n, "description");
        m.model = text(n, "model");
        m.subLabel = text(n, "sub_label");
        m.attribute = text(n, "attribute");
        m.timestamp = number(n, "timestamp");
        return m;
    }

    private static Optional<JsonNode> json(byte[] value) {
        try {
            JsonNode n = JSON.readTree(value);
            return n != null && n.isObject() ? Optional.of(n) : Optional.empty();
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.path(field);
        return v.isValueNode() && !v.isNull() ? v.asText() : null;
    }

    private static Double number(JsonNode n, String field) {
        JsonNode v = n.path(field);
        return v.isNumber() ? v.asDouble() : null;
    }

    private static Boolean bool(JsonNode n, String field) {
        JsonNode v = n.path(field);
        return v.isBoolean() ? v.asBoolean() : null;
    }

    private static String[] strings(JsonNode n) {
        List<String> out = new ArrayList<>();
        if (n.isArray()) {
            n.forEach(v -> {
                if (v.isValueNode() && !v.isNull()) {
                    out.add(v.asText());
                }
            });
        }
        return out.toArray(new String[0]);
    }
}

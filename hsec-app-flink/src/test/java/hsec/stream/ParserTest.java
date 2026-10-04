package hsec.stream;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class ParserTest {
    private static final long NOW = 1_700_000_000_000L;

    @Test
    void parsesAnEvent() {
        Envelope e = Parser.parse(Samples.raw("frigate.events", Samples.EVENT_NEW), NOW).orElseThrow();
        assertThat(e.kind).isEqualTo(Envelope.EVENT);
        assertThat(e.event.type).isEqualTo("new");
        assertThat(e.event.id).isEqualTo("1607123955.475377-mxklsc");
        assertThat(e.event.camera).isEqualTo("front_door");
        assertThat(e.event.label).isEqualTo("person");
        assertThat(e.event.subLabel).isEqualTo("John");
        assertThat(e.event.subLabelScore).isEqualTo(0.91);
        assertThat(e.event.topScore).isEqualTo(0.958984375);
        assertThat(e.event.currentZones).containsExactly("driveway");
        assertThat(e.event.enteredZones).containsExactly("yard", "driveway");
        assertThat(e.event.faceScore).isEqualTo(0.86);
        assertThat(e.event.hasSnapshot).isFalse();
        assertThat(e.event.startTime).isEqualTo(1607123955.475377);
        assertThat(e.event.endTime).isNull();
        assertThat(e.event.frameTime).isEqualTo(1607123961.837752);
        assertThat(e.ingestMs).isEqualTo(NOW);
    }

    @Test
    void parsesAPlainTextSubLabelAndMissingFields() {
        String body = "{\"type\":\"update\",\"after\":{\"id\":\"e1\",\"camera\":\"garage\",\"sub_label\":\"Ann\"}}";
        EventMsg m = Parser.parse(Samples.raw("frigate.events", body), NOW).orElseThrow().event;
        assertThat(m.subLabel).isEqualTo("Ann");
        assertThat(m.subLabelScore).isNull();
        assertThat(m.label).isNull();
        assertThat(m.faceScore).isNull();
        assertThat(m.currentZones).isEmpty();
        assertThat(m.startTime).isNull();
    }

    @Test
    void parsesAReview() {
        Envelope e = Parser.parse(Samples.raw("frigate.reviews", Samples.REVIEW_NEW), NOW).orElseThrow();
        assertThat(e.kind).isEqualTo(Envelope.REVIEW);
        assertThat(e.review.id).isEqualTo("1718987129.308396-fqk5ka");
        assertThat(e.review.severity).isEqualTo("alert");
        assertThat(e.review.detections).containsExactly("1718987128.947436-g92ztx", "1718987148.879516-d7oq7r");
        assertThat(e.review.objects).containsExactly("person", "car");
        assertThat(e.review.subLabels).isEmpty();
        assertThat(e.review.zones).containsExactly("front_yard");
        assertThat(e.review.endTime).isNull();
    }

    @Test
    void parsesAFaceUpdate() {
        Envelope e = Parser.parse(Samples.raw("frigate.tracked_object_update", Samples.FACE_UPDATE), NOW).orElseThrow();
        assertThat(e.kind).isEqualTo(Envelope.OBJECT_UPDATE);
        assertThat(e.update.type).isEqualTo("face");
        assertThat(e.update.name).isEqualTo("John");
        assertThat(e.update.score).isEqualTo(0.95);
        assertThat(e.update.camera).isEqualTo("front_door_cam");
        assertThat(e.update.timestamp).isEqualTo(1607123958.748393);
        assertThat(e.update.plate).isNull();
    }

    @Test
    void keepsADescriptionUpdateWithoutCameraAndTime() {
        Envelope e = Parser.parse(Samples.raw("frigate.tracked_object_update", Samples.DESCRIPTION_UPDATE), NOW)
                .orElseThrow();
        assertThat(e.update.description).startsWith("The car is a red sedan");
        assertThat(e.update.camera).isNull();
        assertThat(e.update.timestamp).isNull();
    }

    @Test
    void parsesATriggerWithTheBridgeReceiveTime() {
        RawRecord r = Samples.raw("frigate.triggers", Samples.TRIGGER);
        r.videoFrom = 1752857210.5;
        Envelope e = Parser.parse(r, NOW).orElseThrow();
        assertThat(e.kind).isEqualTo(Envelope.TRIGGER);
        assertThat(e.trigger.eventId).isEqualTo("1752857206.193062-xyz");
        assertThat(e.trigger.camera).isEqualTo("driveway");
        assertThat(e.trigger.receiveTime).isEqualTo(1752857210.5);
    }

    @Test
    void keepsSystemMessagesAsRawText() {
        Envelope plain = Parser.parse(Samples.raw("frigate.available", "online"), NOW).orElseThrow();
        assertThat(plain.kind).isEqualTo(Envelope.SYSTEM);
        assertThat(plain.topic).isEqualTo("frigate.available");
        assertThat(plain.system.payload).isEqualTo("online");

        Envelope json = Parser.parse(Samples.raw("frigate.stats", "{\"service\":{\"uptime\":5}}"), NOW).orElseThrow();
        assertThat(json.system.payload).isEqualTo("{\"service\":{\"uptime\":5}}");
    }

    @Test
    void dropsCameraRecordsThatDoNotParse() {
        assertThat(Parser.parse(Samples.raw("frigate.events", "not json"), NOW)).isEmpty();
        assertThat(Parser.parse(Samples.raw("frigate.events", "[1,2]"), NOW)).isEmpty();
        assertThat(Parser.parse(Samples.raw("frigate.events", "{\"type\":\"new\"}"), NOW)).isEmpty();
        assertThat(Parser.parse(Samples.raw("frigate.reviews", "{\"after\":{\"id\":\"r1\"}}"), NOW)).isEmpty();
        assertThat(Parser.parse(Samples.raw("frigate.tracked_object_update", "{\"type\":\"face\"}"), NOW)).isEmpty();
        assertThat(Parser.parse(Samples.raw("frigate.triggers", "{\"camera\":\"c\"}"), NOW)).isEmpty();
        assertThat(Parser.parse(Samples.raw("frigate.events", ""), NOW)).isEmpty();
    }

    @Test
    void messageIdDependsOnTopicAndBytes() {
        String a = Parser.messageId("frigate.events", "x".getBytes());
        assertThat(a).hasSize(64).isEqualTo(Parser.messageId("frigate.events", "x".getBytes()));
        assertThat(a).isNotEqualTo(Parser.messageId("frigate.reviews", "x".getBytes()));
        assertThat(a).isNotEqualTo(Parser.messageId("frigate.events", "y".getBytes()));
        // The zero byte keeps topic "ab" + body "c" apart from topic "a" + body "bc".
        assertThat(Parser.messageId("ab", "c".getBytes())).isNotEqualTo(Parser.messageId("a", "bc".getBytes()));
        Optional<Envelope> e = Parser.parse(Samples.raw("frigate.available", "online"), NOW);
        assertThat(e.orElseThrow().messageId).isEqualTo(Parser.messageId("frigate.available", "online".getBytes()));
    }
}

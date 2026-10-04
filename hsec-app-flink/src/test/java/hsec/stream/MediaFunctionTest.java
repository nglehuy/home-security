package hsec.stream;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class MediaFunctionTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JobConfig CONFIG = new JobConfig(ZoneId.of("Asia/Singapore"), "https://192.168.1.17:8971",
            "http://unused", "unused:9093", Map.of());

    /** A fake Frigate: the snapshot and the clip are fixed bytes, or null for a failed download. */
    static class FakeFrigate extends FrigateClient {
        private final byte[] snapshot;
        private final byte[] clip;
        volatile String lastClip;

        FakeFrigate(byte[] snapshot, byte[] clip) {
            super("http://unused");
            this.snapshot = snapshot;
            this.clip = clip;
        }

        @Override
        public byte[] snapshot(String eventId) {
            return snapshot;
        }

        @Override
        public byte[] clip(String camera, double from, double to) {
            lastClip = camera + " " + Times.format(from) + " " + Times.format(to);
            return clip;
        }
    }

    static class Capture implements ResultFuture<AlertMessage> {
        final CompletableFuture<AlertMessage> result = new CompletableFuture<>();

        @Override
        public void complete(Collection<AlertMessage> r) {
            result.complete(r.iterator().next());
        }

        @Override
        public void complete(CollectionSupplier<AlertMessage> r) {
            try {
                complete(r.get());
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        }

        @Override
        public void completeExceptionally(Throwable t) {
            result.completeExceptionally(t);
        }
    }

    private static AlertRequest start(String eventId) {
        AlertRequest r = new AlertRequest();
        r.alertId = "r1:start";
        r.kind = AlertRequest.START;
        r.reviewId = "r1";
        r.camera = "front_cam";
        r.severity = "alert";
        r.objects = new String[] {"person", "car"};
        r.subLabels = new String[0];
        r.zones = new String[] {"front_yard"};
        r.reviewStart = 1718987129.308396;
        r.eventId = eventId;
        r.firedMs = 1_791_000_000_120L;
        r.from = r.reviewStart;
        r.to = r.reviewStart;
        return r;
    }

    private static AlertRequest clip() {
        AlertRequest r = start(null);
        r.alertId = "r1:clip:2";
        r.kind = AlertRequest.CLIP;
        r.part = 2;
        r.partCount = 3;
        r.from = 1718987154.308396;
        r.to = 1718987179.308396;
        return r;
    }

    private static AlertMessage run(FrigateClient f, AlertRequest r) throws Exception {
        MediaFunction fn = new MediaFunction(f, CONFIG);
        fn.open(null);
        try {
            Capture c = new Capture();
            fn.asyncInvoke(r, c);
            return c.result.get(10, TimeUnit.SECONDS);
        } finally {
            fn.close();
        }
    }

    private static JsonNode json(AlertMessage m) throws Exception {
        return JSON.readTree(m.toJson());
    }

    @Test
    void jpegAnswerGivesTheSameBytesInBase64() throws Exception {
        AlertMessage m = run(new FakeFrigate(FrigateClientTest.JPEG, null), start("e1"));
        JsonNode j = json(m);
        assertThat(Base64.getDecoder().decode(j.get("image_jpeg").asText())).isEqualTo(FrigateClientTest.JPEG);
        assertThat(j.get("snapshot_url").asText()).isEqualTo("https://192.168.1.17:8971/api/events/e1/snapshot.jpg");
        assertThat(j.get("fired_ts").asText()).isEqualTo("2026-10-03T12:00:00.120+08:00");
        assertThat(j.get("objects")).hasSize(2);
        assertThat(j.has("clip_mp4")).isFalse();
    }

    @Test
    void mp4AnswerGivesTheSameBytesForTheExactWindow() throws Exception {
        FakeFrigate f = new FakeFrigate(null, FrigateClientTest.MP4);
        AlertMessage m = run(f, clip());
        JsonNode j = json(m);
        assertThat(f.lastClip).isEqualTo("front_cam 1718987154.308396 1718987179.308396");
        assertThat(Base64.getDecoder().decode(j.get("clip_mp4").asText())).isEqualTo(FrigateClientTest.MP4);
        assertThat(j.get("part").asInt()).isEqualTo(2);
        assertThat(j.get("part_count").asInt()).isEqualTo(3);
        assertThat(j.get("clip_url").asText()).isEqualTo(
                "https://192.168.1.17:8971/api/front_cam/start/1718987154.308396/end/1718987179.308396/clip.mp4");
    }

    @Test
    void failedDownloadsStillSendTheMessage() throws Exception {
        JsonNode s = json(run(new FakeFrigate(null, null), start("e1")));
        assertThat(s.get("image_jpeg").isNull()).isTrue();
        JsonNode c = json(run(new FakeFrigate(null, null), clip()));
        assertThat(c.get("clip_mp4").isNull()).isTrue();
        assertThat(c.get("clip_url").asText()).endsWith("/clip.mp4");
    }

    @Test
    void reviewWithoutAnObjectSendsNoSnapshot() throws Exception {
        JsonNode j = json(run(new FakeFrigate(FrigateClientTest.JPEG, null), start(null)));
        assertThat(j.get("image_jpeg").isNull()).isTrue();
        assertThat(j.get("snapshot_url").isNull()).isTrue();
    }

    @Test
    void timeoutSendsTheMessageWithoutTheFile() throws Exception {
        MediaFunction fn = new MediaFunction(new FakeFrigate(null, null), CONFIG);
        Capture c = new Capture();
        fn.timeout(clip(), c);
        assertThat(c.result.get(1, TimeUnit.SECONDS).clipMp4).isNull();
    }

    @Test
    void everyMessageHasTheVideoFieldAndHeaders() throws Exception {
        for (AlertMessage m : new AlertMessage[] {
                run(new FakeFrigate(null, null), start("e1")), run(new FakeFrigate(null, null), clip())}) {
            JsonNode v = json(m).get("video");
            assertThat(v.get("camera").asText()).isEqualTo("front_cam");
            assertThat(v.get("from").asDouble()).isEqualTo(m.videoFrom);
            assertThat(v.get("to").asDouble()).isEqualTo(m.videoTo);

            ProducerRecord<byte[], byte[]> rec = new AlertKafkaSerializer().serialize(m, null, null);
            assertThat(rec.topic()).isEqualTo("hsec.alerts");
            assertThat(new String(rec.key(), StandardCharsets.UTF_8)).isEqualTo(m.alertId);
            assertThat(header(rec, "video_camera")).isEqualTo("front_cam");
            assertThat(header(rec, "video_from")).isEqualTo(Times.format(m.videoFrom));
            assertThat(header(rec, "video_to")).isEqualTo(Times.format(m.videoTo));
        }
    }

    private static String header(ProducerRecord<byte[], byte[]> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}

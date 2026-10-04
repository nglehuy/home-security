package hsec.stream;

import java.nio.charset.StandardCharsets;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

/** Writes alert messages to hsec.alerts with the alert ID as key and the video_* headers. */
public class AlertKafkaSerializer implements KafkaRecordSerializationSchema<AlertMessage> {
    private static final long serialVersionUID = 1L;
    public static final String TOPIC = "hsec.alerts";

    @Override
    public ProducerRecord<byte[], byte[]> serialize(AlertMessage m, KafkaSinkContext context, Long timestamp) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("video_camera", m.camera.getBytes(StandardCharsets.UTF_8));
        headers.add("video_from", Times.format(m.videoFrom).getBytes(StandardCharsets.UTF_8));
        headers.add("video_to", Times.format(m.videoTo).getBytes(StandardCharsets.UTF_8));
        return new ProducerRecord<>(TOPIC, null, null,
                m.alertId.getBytes(StandardCharsets.UTF_8), m.toJson(), headers);
    }
}

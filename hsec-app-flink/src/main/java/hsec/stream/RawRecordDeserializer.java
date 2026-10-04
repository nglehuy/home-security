package hsec.stream;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/** Turns a Kafka record into a RawRecord. It keeps the value bytes as they are. */
public class RawRecordDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawRecord> out) {
        byte[] value = record.value() == null ? new byte[0] : record.value();
        out.collect(new RawRecord(
                record.topic(),
                value,
                header(record, "video_camera"),
                number(header(record, "video_from")),
                number(header(record, "video_to"))));
    }

    @Override
    public TypeInformation<RawRecord> getProducedType() {
        return TypeInformation.of(RawRecord.class);
    }

    private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static Double number(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Double.valueOf(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

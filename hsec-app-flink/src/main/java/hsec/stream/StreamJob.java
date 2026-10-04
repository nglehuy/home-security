package hsec.stream;

import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** The stream job: Frigate messages from Redpanda into Iceberg, and alerts to hsec.alerts. */
public final class StreamJob {
    private StreamJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args, System.getenv());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Start from the committed offsets, or from the earliest offset on the first run.
        KafkaSource<RawRecord> source = KafkaSource.<RawRecord>builder()
                .setBootstrapServers(config.kafkaBootstrap)
                .setTopicPattern(Pattern.compile("frigate\\..*"))
                .setGroupId("hsec-stream")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new RawRecordDeserializer())
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();
        DataStream<RawRecord> raw = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "frigate.* topics")
                .uid("source");

        // An alert message can carry a 19 MiB clip in base64, so allow requests of up to 32 MiB.
        Properties producer = new Properties();
        producer.setProperty("max.request.size", String.valueOf(32 << 20));
        producer.setProperty("buffer.memory", String.valueOf(64 << 20));
        KafkaSink<AlertMessage> alerts = KafkaSink.<AlertMessage>builder()
                .setBootstrapServers(config.kafkaBootstrap)
                .setRecordSerializer(new AlertKafkaSerializer())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producer)
                .build();

        CatalogLoader catalog = CatalogLoader.custom(
                "lake", config.catalogProperties, new Configuration(), "org.apache.iceberg.jdbc.JdbcCatalog");

        Pipeline.build(raw, config, catalog, new FrigateClient(config.frigateApi), alerts);
        env.execute("hsec-stream");
    }
}

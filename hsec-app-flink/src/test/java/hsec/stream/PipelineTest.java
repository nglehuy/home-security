package hsec.stream;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.io.CloseableIterable;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

/** Runs the whole job graph on a Flink MiniCluster, with Iceberg tables in a temporary folder. */
class PipelineTest {
    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(1)
                    .build());

    /** A 5 MB clip part, bigger than any default Kafka or Parquet buffer. */
    static final byte[] CLIP = new byte[5_000_000];
    static final ConcurrentLinkedQueue<AlertMessage> SENT = new ConcurrentLinkedQueue<>();
    static HttpServer frigate;

    @TempDir
    static Path warehouse;

    @BeforeAll
    static void startFrigate() throws Exception {
        new Random(7).nextBytes(CLIP);
        frigate = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        frigate.createContext("/", ex -> {
            byte[] body = ex.getRequestURI().getPath().endsWith("clip.mp4") ? CLIP : FrigateClientTest.JPEG;
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream o = ex.getResponseBody()) {
                o.write(body);
            }
        });
        frigate.start();
    }

    @AfterAll
    static void stopFrigate() {
        frigate.stop(0);
    }

    /** Collects the alert messages in place of the Kafka sink. */
    static class CollectSink implements Sink<AlertMessage> {
        @Override
        public SinkWriter<AlertMessage> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(AlertMessage m, Context c) {
                    SENT.add(m);
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }

    @Test
    void writesAllTablesAndSendsTheAlertWithItsClip() throws Exception {
        CatalogLoader loader = CatalogLoader.hadoop(
                "lake", new Configuration(), Map.of("warehouse", warehouse.toUri().toString()));
        Catalog catalog = loader.loadCatalog();
        for (String name : Tables.SCHEMAS.keySet()) {
            var schema = Tables.schema(name);
            catalog.buildTable(TableIdentifier.of(Tables.NAMESPACE, name), schema)
                    .withPartitionSpec(Tables.spec(schema))
                    .withSortOrder(Tables.sortOrder(name, schema))
                    .withProperties(Tables.PROPERTIES)
                    // WRITE ORDERED BY in the migration also sets this property.
                    .withProperty("write.distribution-mode", "range")
                    .create();
        }

        // The review ended long ago, so its clip window is ready at once.
        String event = "{\"type\":\"new\",\"after\":{\"id\":\"p1\",\"camera\":\"front\",\"label\":\"person\","
                + "\"top_score\":0.9,\"start_time\":1000.5,\"end_time\":null,\"frame_time\":1001.0}}";
        String reviewNew = "{\"type\":\"new\",\"after\":{\"id\":\"r1\",\"camera\":\"front\",\"start_time\":1000.0,"
                + "\"end_time\":null,\"severity\":\"alert\",\"data\":{\"detections\":[\"p1\"],\"objects\":[\"person\"],"
                + "\"sub_labels\":[],\"zones\":[\"yard\"]}}}";
        String reviewEnd = reviewNew.replace("\"type\":\"new\"", "\"type\":\"end\"")
                .replace("\"end_time\":null", "\"end_time\":1010.0");
        List<RawRecord> input = new ArrayList<>(List.of(
                Samples.raw("frigate.events", event),
                Samples.raw("frigate.reviews", reviewNew),
                Samples.raw("frigate.reviews", reviewEnd),
                Samples.raw("frigate.available", "online"),
                Samples.raw("frigate.available", "online"),
                Samples.raw("frigate.events", "not json")));

        JobConfig config = new JobConfig(ZoneId.of("Asia/Singapore"), "https://192.168.1.17:8971",
                "http://127.0.0.1:" + frigate.getAddress().getPort(), "unused:9093", Map.of());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Pipeline.build(env.fromData(input), config, loader, new FrigateClient(config.frigateApi), new CollectSink());
        env.execute("pipeline-test");

        assertThat(rows(catalog, Tables.EVENTS)).hasSize(1);
        assertThat(rows(catalog, Tables.REVIEWS)).hasSize(2);
        // The duplicate system message is dropped.
        assertThat(rows(catalog, Tables.SYSTEM_MESSAGES)).hasSize(1);

        List<Record> alerts = rows(catalog, Tables.ALERTS);
        assertThat(alerts).extracting(r -> r.getField("alert_id")).containsExactlyInAnyOrder("r1:start", "r1:clip:1");
        Record clip = alerts.stream().filter(r -> "clip".equals(r.getField("kind"))).findFirst().orElseThrow();
        assertThat(bytes(clip.getField("clip"))).isEqualTo(CLIP);
        assertThat(clip.getField("image")).isNull();
        Record start = alerts.stream().filter(r -> "start".equals(r.getField("kind"))).findFirst().orElseThrow();
        assertThat(bytes(start.getField("image"))).isEqualTo(FrigateClientTest.JPEG);
        assertThat(start.getField("event_id")).isEqualTo("p1");

        // Message 1 leaves before the clip part.
        assertThat(SENT).extracting(m -> m.alertId).containsExactly("r1:start", "r1:clip:1");
        assertThat(new String(SENT.peek().toJson(), StandardCharsets.UTF_8)).contains("\"image_jpeg\":\"/9j/4AECAw==\"");
    }

    private static List<Record> rows(Catalog catalog, String name) throws Exception {
        Table t = catalog.loadTable(TableIdentifier.of(Tables.NAMESPACE, name));
        List<Record> out = new ArrayList<>();
        try (CloseableIterable<Record> it = IcebergGenerics.read(t).build()) {
            it.forEach(out::add);
        }
        return out;
    }

    private static byte[] bytes(Object value) {
        ByteBuffer b = ((ByteBuffer) value).duplicate();
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }
}

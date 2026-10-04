package hsec.stream;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

/** Builds the job graph. StreamJob gives it the real source and sinks, and the tests give it test ones. */
public final class Pipeline {
    /** At most 2 clips and 10 snapshots download at once, so 12 requests can wait for media. */
    public static final int MEDIA_CAPACITY = MediaFunction.CLIP_THREADS + MediaFunction.SNAPSHOT_THREADS;
    /** Long enough for 12 queued clips with a 30-second timeout each on 2 threads. */
    public static final long MEDIA_TIMEOUT_MINUTES = 5;

    private Pipeline() {}

    public static void build(DataStream<RawRecord> raw, JobConfig config, CatalogLoader catalog,
            FrigateClient frigate, Sink<AlertMessage> alertSink) {
        DataStream<Envelope> unique = raw
                .process(new ParseFunction()).uid("parse").name("Parse")
                .keyBy(e -> e.messageId)
                .process(new Dedupe()).uid("dedupe").name("Deduplicate");

        iceberg(rows(unique, Envelope.EVENT, Tables.EVENTS), catalog, Tables.EVENTS);
        iceberg(rows(unique, Envelope.REVIEW, Tables.REVIEWS), catalog, Tables.REVIEWS);
        iceberg(rows(unique, Envelope.OBJECT_UPDATE, Tables.OBJECT_UPDATES), catalog, Tables.OBJECT_UPDATES);
        iceberg(rows(unique, Envelope.TRIGGER, Tables.TRIGGERS), catalog, Tables.TRIGGERS);
        iceberg(rows(unique, Envelope.SYSTEM, Tables.SYSTEM_MESSAGES), catalog, Tables.SYSTEM_MESSAGES);

        DataStream<EventMsg> events = unique
                .filter(e -> Envelope.EVENT.equals(e.kind)).name("Events")
                .map(e -> e.event).returns(EventMsg.class).name("Event messages");
        DataStream<ReviewMsg> reviews = unique
                .filter(e -> Envelope.REVIEW.equals(e.kind)).name("Reviews")
                .map(e -> e.review).returns(ReviewMsg.class).name("Review messages");

        DataStream<RowData> visits = events
                .filter(e -> "person".equals(e.label)).name("People")
                .keyBy(e -> e.camera)
                .process(new VisitFunction()).uid("visits").name("Visits")
                .map(Rows::visit).returns(rowType(Tables.VISITS)).name("Visit rows");
        iceberg(visits, catalog, Tables.VISITS);

        DataStream<AlertRequest> requests = events
                .connect(reviews)
                .keyBy(e -> e.camera, r -> r.camera)
                .process(new AlertFunction()).uid("alerts").name("Alerts");

        // Ordered, so message 1 of a review always leaves before its clip parts.
        DataStream<AlertMessage> messages = AsyncDataStream
                .orderedWait(requests, new MediaFunction(frigate, config), MEDIA_TIMEOUT_MINUTES, TimeUnit.MINUTES,
                        MEDIA_CAPACITY)
                .uid("media").name("Alert media");

        messages.sinkTo(alertSink).uid("alerts-kafka").name("hsec.alerts");
        iceberg(messages.map(Rows::alert).returns(rowType(Tables.ALERTS)).name("Alert rows"), catalog, Tables.ALERTS);
    }

    private static DataStream<RowData> rows(DataStream<Envelope> unique, String kind, String table) {
        DataStream<Envelope> picked = unique.filter(e -> kind.equals(e.kind)).name(table);
        return switch (kind) {
            case Envelope.EVENT -> picked.map(Rows::event).returns(rowType(table)).name(table + " rows");
            case Envelope.REVIEW -> picked.map(Rows::review).returns(rowType(table)).name(table + " rows");
            case Envelope.OBJECT_UPDATE -> picked.map(Rows::objectUpdate).returns(rowType(table)).name(table + " rows");
            case Envelope.TRIGGER -> picked.map(Rows::trigger).returns(rowType(table)).name(table + " rows");
            default -> picked.map(Rows::system).returns(rowType(table)).name(table + " rows");
        };
    }

    static TypeInformation<RowData> rowType(String table) {
        return InternalTypeInfo.of(FlinkSchemaUtil.convert(Tables.schema(table)));
    }

    /**
     * Appends to an existing table. Iceberg commits once per checkpoint. The migrations
     * set a sort order, which also sets write.distribution-mode=range. One writer needs
     * no range shuffle, so the sink does not distribute. The nightly compaction sorts.
     */
    private static void iceberg(DataStream<RowData> rows, CatalogLoader catalog, String table) {
        FlinkSink.forRowData(rows)
                .tableLoader(TableLoader.fromCatalog(catalog, TableIdentifier.of(Tables.NAMESPACE, table)))
                .distributionMode(DistributionMode.NONE)
                .writeParallelism(1)
                .uidPrefix("iceberg-" + table)
                .append();
    }
}

package hsec.stream;

import java.util.Optional;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parses each record. A camera record that does not parse increases a metric and leaves the stream. */
public class ParseFunction extends ProcessFunction<RawRecord, Envelope> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ParseFunction.class);

    private transient Counter failures;

    @Override
    public void open(OpenContext ctx) {
        failures = getRuntimeContext().getMetricGroup().counter("parseFailures");
    }

    @Override
    public void processElement(RawRecord r, Context ctx, Collector<Envelope> out) {
        Optional<Envelope> e = Parser.parse(r, ctx.timerService().currentProcessingTime());
        if (e.isPresent()) {
            out.collect(e.get());
        } else {
            failures.inc();
            LOG.warn("Dropped a record on {} that does not parse. Redpanda keeps it for 7 days.", r.topic);
        }
    }
}

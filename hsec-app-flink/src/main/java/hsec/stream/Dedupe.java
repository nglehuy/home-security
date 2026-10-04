package hsec.stream;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Drops a message whose ID it saw in the last 10 minutes. Keyed by message ID. */
public class Dedupe extends KeyedProcessFunction<String, Envelope, Envelope> {
    private static final long serialVersionUID = 1L;
    public static final Duration TTL = Duration.ofMinutes(10);

    private transient ValueState<Boolean> seen;

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<Boolean> d = new ValueStateDescriptor<>("seen", Boolean.class);
        d.enableTimeToLive(StateTtlConfig.newBuilder(TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        seen = getRuntimeContext().getState(d);
    }

    @Override
    public void processElement(Envelope e, Context ctx, Collector<Envelope> out) throws Exception {
        if (seen.value() == null) {
            seen.update(true);
            out.collect(e);
        }
    }
}

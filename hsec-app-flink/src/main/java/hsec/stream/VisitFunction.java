package hsec.stream;

import java.time.Duration;
import java.util.Map;
import java.util.TreeSet;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Builds visits from person events, keyed by camera. A visit ends 5 minutes after
 * the last person leaves, if no new person comes in that time.
 */
public class VisitFunction extends KeyedProcessFunction<String, EventMsg, Visit> {
    private static final long serialVersionUID = 1L;
    public static final Duration GAP = Duration.ofMinutes(5);

    /** Tracked person IDs that are still in view. */
    private transient MapState<String, Boolean> active;
    /** Every person ID of the visit, with its recognized name, or "" for no name. */
    private transient MapState<String, String> people;
    private transient MapState<String, Boolean> zones;
    private transient ValueState<Double> start;
    private transient ValueState<Double> end;
    private transient ValueState<Long> timer;

    @Override
    public void open(OpenContext ctx) {
        active = getRuntimeContext().getMapState(new MapStateDescriptor<>("active", String.class, Boolean.class));
        people = getRuntimeContext().getMapState(new MapStateDescriptor<>("people", String.class, String.class));
        zones = getRuntimeContext().getMapState(new MapStateDescriptor<>("zones", String.class, Boolean.class));
        start = getRuntimeContext().getState(new ValueStateDescriptor<>("start", Double.class));
        end = getRuntimeContext().getState(new ValueStateDescriptor<>("end", Double.class));
        timer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
    }

    @Override
    public void processElement(EventMsg e, Context ctx, Collector<Visit> out) throws Exception {
        if (!"person".equals(e.label) || e.id == null) {
            return;
        }
        String name = e.subLabel == null ? "" : e.subLabel;
        String known = people.get(e.id);
        if (known == null || (known.isEmpty() && !name.isEmpty())) {
            people.put(e.id, name);
        }
        addZones(e.enteredZones);
        addZones(e.currentZones);
        widen(e.startTime);
        widen(e.frameTime);
        widen(e.endTime);

        if ("end".equals(e.type)) {
            active.remove(e.id);
            if (active.isEmpty()) {
                long at = ctx.timerService().currentProcessingTime() + GAP.toMillis();
                timer.update(at);
                ctx.timerService().registerProcessingTimeTimer(at);
            }
        } else {
            active.put(e.id, true);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Visit> out) throws Exception {
        Long latest = timer.value();
        // An older timer, or a person came back in the meantime.
        if (latest == null || latest != timestamp || !active.isEmpty()) {
            return;
        }
        Visit v = new Visit();
        v.camera = ctx.getCurrentKey();
        v.startTime = start.value();
        v.endTime = end.value();
        v.visitId = v.camera + ":" + Times.toMillis(v.startTime);
        TreeSet<String> names = new TreeSet<>();
        int unknown = 0;
        int count = 0;
        for (Map.Entry<String, String> p : people.entries()) {
            count++;
            if (p.getValue().isEmpty()) {
                unknown++;
            } else {
                names.add(p.getValue());
            }
        }
        v.personCount = count;
        v.unknownCount = unknown;
        v.knownNames = names.toArray(new String[0]);
        TreeSet<String> z = new TreeSet<>();
        for (String k : zones.keys()) {
            z.add(k);
        }
        v.zones = z.toArray(new String[0]);
        v.ingestMs = timestamp;
        out.collect(v);

        active.clear();
        people.clear();
        zones.clear();
        start.clear();
        end.clear();
        timer.clear();
    }

    private void addZones(String[] values) throws Exception {
        if (values != null) {
            for (String z : values) {
                zones.put(z, true);
            }
        }
    }

    private void widen(Double t) throws Exception {
        if (t == null) {
            return;
        }
        if (start.value() == null || t < start.value()) {
            start.update(t);
        }
        if (end.value() == null || t > end.value()) {
            end.update(t);
        }
    }
}

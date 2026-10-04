package hsec.stream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Turns Frigate alert reviews into alert requests, keyed by camera.
 * Message 1 asks for a snapshot as soon as a review becomes an alert. When the
 * review ends, the review video is cut into 25-second windows, and each window
 * asks for its clip 20 seconds after the window ends.
 */
public class AlertFunction extends KeyedCoProcessFunction<String, EventMsg, ReviewMsg, AlertRequest> {
    private static final long serialVersionUID = 1L;
    public static final double WINDOW_SECONDS = 25;
    /** Frigate needs up to about 17 seconds to finish and store a recording segment. */
    public static final double READY_DELAY_SECONDS = 20;
    private static final Duration STATE_TTL = Duration.ofDays(1);

    /** Tracked objects of the camera: event ID to label and top score. */
    private transient MapState<String, TrackedObject> objects;
    /** Review IDs that already sent message 1. */
    private transient MapState<String, Boolean> alerted;
    /** Clip windows that wait for their video, by alert ID. */
    private transient MapState<String, AlertRequest> pending;

    /** Label and top score of one tracked object. */
    public static class TrackedObject {
        public String label;
        public Double topScore;

        public TrackedObject() {}

        public TrackedObject(String label, Double topScore) {
            this.label = label;
            this.topScore = topScore;
        }
    }

    @Override
    public void open(OpenContext ctx) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(STATE_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<String, TrackedObject> o =
                new MapStateDescriptor<>("objects", String.class, TrackedObject.class);
        o.enableTimeToLive(ttl);
        MapStateDescriptor<String, Boolean> a = new MapStateDescriptor<>("alerted", String.class, Boolean.class);
        a.enableTimeToLive(ttl);
        objects = getRuntimeContext().getMapState(o);
        alerted = getRuntimeContext().getMapState(a);
        pending = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("pending", String.class, AlertRequest.class));
    }

    @Override
    public void processElement1(EventMsg e, Context ctx, Collector<AlertRequest> out) throws Exception {
        if (e.id != null) {
            objects.put(e.id, new TrackedObject(e.label, e.topScore));
        }
    }

    @Override
    public void processElement2(ReviewMsg r, Context ctx, Collector<AlertRequest> out) throws Exception {
        if (!"alert".equals(r.severity) || r.id == null || r.startTime == null) {
            return;
        }
        long now = ctx.timerService().currentProcessingTime();

        if (!alerted.contains(r.id)) {
            out.collect(start(r, pickObject(r.detections), now));
            alerted.put(r.id, true);
        }

        if ("end".equals(r.type)) {
            for (AlertRequest clip : clips(r, now)) {
                if (clip.readyMs <= now) {
                    out.collect(clip);
                } else {
                    pending.put(clip.alertId, clip);
                    ctx.timerService().registerProcessingTimeTimer(clip.readyMs);
                }
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertRequest> out) throws Exception {
        List<AlertRequest> ready = new ArrayList<>();
        for (Map.Entry<String, AlertRequest> p : pending.entries()) {
            if (p.getValue().readyMs <= timestamp) {
                ready.add(p.getValue());
            }
        }
        ready.sort(Comparator.comparingLong((AlertRequest c) -> c.readyMs).thenComparingInt(c -> c.part));
        for (AlertRequest c : ready) {
            pending.remove(c.alertId);
            out.collect(c);
        }
    }

    /** A person with the highest top score, else the first detection. Null if the review has none. */
    String pickObject(String[] detections) throws Exception {
        if (detections == null || detections.length == 0) {
            return null;
        }
        String best = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (String id : detections) {
            TrackedObject o = objects.get(id);
            if (o != null && "person".equals(o.label)) {
                double s = o.topScore == null ? 0 : o.topScore;
                if (best == null || s > bestScore) {
                    best = id;
                    bestScore = s;
                }
            }
        }
        return best != null ? best : detections[0];
    }

    static AlertRequest start(ReviewMsg r, String eventId, long now) {
        AlertRequest a = base(r, now);
        a.alertId = r.id + ":start";
        a.kind = AlertRequest.START;
        a.eventId = eventId;
        a.from = r.startTime;
        a.to = r.startTime;
        a.readyMs = now;
        return a;
    }

    /**
     * Cuts the review into 25-second windows. The last window can be shorter, and a
     * review always has at least one window.
     */
    static List<AlertRequest> clips(ReviewMsg r, long now) {
        double begin = r.startTime;
        double finish = r.endTime != null && r.endTime > begin ? r.endTime : begin;
        int count = Math.max(1, (int) Math.ceil((finish - begin) / WINDOW_SECONDS));
        List<AlertRequest> out = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            AlertRequest a = base(r, now);
            a.alertId = r.id + ":clip:" + i;
            a.kind = AlertRequest.CLIP;
            a.part = i;
            a.partCount = count;
            a.from = begin + WINDOW_SECONDS * (i - 1);
            a.to = Math.min(begin + WINDOW_SECONDS * i, finish);
            a.readyMs = Times.toMillis(a.to + READY_DELAY_SECONDS);
            out.add(a);
        }
        return out;
    }

    private static AlertRequest base(ReviewMsg r, long now) {
        AlertRequest a = new AlertRequest();
        a.reviewId = r.id;
        a.camera = r.camera;
        a.severity = r.severity;
        a.objects = r.objects;
        a.subLabels = r.subLabels;
        a.zones = r.zones;
        a.reviewStart = r.startTime;
        a.firedMs = now;
        return a;
    }
}

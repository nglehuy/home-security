package hsec.stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * Downloads the snapshot or the clip of each alert request, then builds the alert
 * message. At most 2 clips and 10 snapshots download at a time. A failed download
 * gives a message without the file, so the alert still goes out.
 */
public class MediaFunction extends RichAsyncFunction<AlertRequest, AlertMessage> {
    private static final long serialVersionUID = 1L;
    public static final int CLIP_THREADS = 2;
    public static final int SNAPSHOT_THREADS = 10;

    private final FrigateClient frigate;
    private final JobConfig config;
    private transient ExecutorService clips;
    private transient ExecutorService snapshots;

    public MediaFunction(FrigateClient frigate, JobConfig config) {
        this.frigate = frigate;
        this.config = config;
    }

    @Override
    public void open(OpenContext ctx) {
        clips = Executors.newFixedThreadPool(CLIP_THREADS);
        snapshots = Executors.newFixedThreadPool(SNAPSHOT_THREADS);
    }

    @Override
    public void asyncInvoke(AlertRequest r, ResultFuture<AlertMessage> result) {
        boolean clip = AlertRequest.CLIP.equals(r.kind);
        CompletableFuture
                .supplyAsync(() -> fetch(r), clip ? clips : snapshots)
                .whenComplete((file, error) -> result.complete(List.of(message(r, error == null ? file : null))));
    }

    @Override
    public void timeout(AlertRequest r, ResultFuture<AlertMessage> result) {
        result.complete(List.of(message(r, null)));
    }

    @Override
    public void close() {
        if (clips != null) {
            clips.shutdownNow();
        }
        if (snapshots != null) {
            snapshots.shutdownNow();
        }
    }

    private byte[] fetch(AlertRequest r) {
        if (AlertRequest.CLIP.equals(r.kind)) {
            return frigate.clip(r.camera, r.from, r.to);
        }
        return r.eventId == null ? null : frigate.snapshot(r.eventId);
    }

    private AlertMessage message(AlertRequest r, byte[] file) {
        return AlertMessage.from(r, file, config.frigateUrl, config.zone, System.currentTimeMillis());
    }
}

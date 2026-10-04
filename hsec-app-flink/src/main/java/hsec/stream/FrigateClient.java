package hsec.stream;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads snapshots and clips from the Frigate API on port 5000.
 * Each method returns null on a timeout, an HTTP error, or a file above its size limit.
 */
public class FrigateClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FrigateClient.class);

    public static final int SNAPSHOT_LIMIT_BYTES = 1 << 20;
    public static final int CLIP_LIMIT_BYTES = 19 << 20;

    private final String api;
    private final Duration snapshotTimeout;
    private final Duration clipTimeout;
    private transient HttpClient http;

    public FrigateClient(String api) {
        this(api, Duration.ofSeconds(5), Duration.ofSeconds(30));
    }

    FrigateClient(String api, Duration snapshotTimeout, Duration clipTimeout) {
        this.api = api.endsWith("/") ? api.substring(0, api.length() - 1) : api;
        this.snapshotTimeout = snapshotTimeout;
        this.clipTimeout = clipTimeout;
    }

    /** The 720-pixel snapshot of a tracked object, with its bounding box. It works while the object is in progress. */
    public byte[] snapshot(String eventId) {
        return get("/api/events/" + eventId + "/snapshot.jpg?bounding_box=1&height=720&quality=70",
                snapshotTimeout, SNAPSHOT_LIMIT_BYTES);
    }

    /** The recorded video of a camera between two Unix times. */
    public byte[] clip(String camera, double from, double to) {
        return get("/api/" + camera + "/start/" + Times.format(from) + "/end/" + Times.format(to) + "/clip.mp4",
                clipTimeout, CLIP_LIMIT_BYTES);
    }

    private byte[] get(String path, Duration timeout, int limit) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(api + path)).timeout(timeout).GET().build();
        HttpResponse.BodyHandler<byte[]> handler = info -> {
            long length = info.headers().firstValueAsLong("content-length").orElse(-1);
            if (info.statusCode() != 200 || length > limit) {
                return HttpResponse.BodySubscribers.replacing(null);
            }
            return HttpResponse.BodySubscribers.ofByteArray();
        };
        try {
            HttpResponse<byte[]> resp = client().sendAsync(req, handler)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            byte[] body = resp.body();
            if (resp.statusCode() != 200 || body == null || body.length > limit) {
                LOG.warn("No file from {}: HTTP {}, {} bytes, limit {}.", path, resp.statusCode(),
                        body == null ? "no" : body.length, limit);
                return null;
            }
            return body;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            LOG.warn("No file from {}: {}", path, e.toString());
            return null;
        }
    }

    private synchronized HttpClient client() {
        if (http == null) {
            http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        }
        return http;
    }
}

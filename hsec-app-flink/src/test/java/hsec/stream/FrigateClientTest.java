package hsec.stream;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

class FrigateClientTest {
    static final byte[] JPEG = {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xE0, 1, 2, 3};
    static final byte[] MP4 = {0, 0, 0, 0x20, 'f', 't', 'y', 'p', 9, 9};

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private volatile int status = 200;
    private volatile byte[] body = JPEG;
    private volatile long delayMs;
    private volatile boolean chunked;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            requests.add(ex.getRequestURI().toString());
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ex.sendResponseHeaders(status, chunked ? 0 : body.length);
            try (OutputStream o = ex.getResponseBody()) {
                o.write(body);
            } catch (Exception ignored) {
                // The client can close the connection on a timeout.
            }
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    FrigateClient client() {
        return new FrigateClient("http://127.0.0.1:" + server.getAddress().getPort() + "/",
                Duration.ofMillis(500), Duration.ofSeconds(5));
    }

    @Test
    void snapshotReturnsTheBytesAndAsksFor720Pixels() {
        assertThat(client().snapshot("1607123955.475377-mxklsc")).isEqualTo(JPEG);
        assertThat(requests).containsExactly(
                "/api/events/1607123955.475377-mxklsc/snapshot.jpg?bounding_box=1&height=720&quality=70");
    }

    @Test
    void clipAsksForTheExactWindow() {
        body = MP4;
        assertThat(client().clip("front_cam", 1718987154.308396, 1718987179.308396)).isEqualTo(MP4);
        assertThat(requests).containsExactly(
                "/api/front_cam/start/1718987154.308396/end/1718987179.308396/clip.mp4");
    }

    @Test
    void timeoutGivesNull() {
        delayMs = 1500;
        assertThat(client().snapshot("e1")).isNull();
    }

    @Test
    void httpErrorGivesNull() {
        status = 400;
        assertThat(client().clip("c", 1, 2)).isNull();
        status = 500;
        assertThat(client().snapshot("e1")).isNull();
    }

    @Test
    void imageAboveOneMebibyteGivesNull() {
        body = new byte[FrigateClient.SNAPSHOT_LIMIT_BYTES + 1];
        assertThat(client().snapshot("e1")).isNull();
        body = new byte[FrigateClient.SNAPSHOT_LIMIT_BYTES];
        assertThat(client().snapshot("e1")).hasSize(FrigateClient.SNAPSHOT_LIMIT_BYTES);
    }

    @Test
    void clipAbove19MebibytesGivesNull() {
        body = new byte[FrigateClient.CLIP_LIMIT_BYTES + 1];
        assertThat(client().clip("c", 1, 2)).isNull();
    }

    @Test
    void chunkedBodyAboveTheLimitGivesNull() {
        chunked = true;
        body = new byte[FrigateClient.SNAPSHOT_LIMIT_BYTES + 1];
        assertThat(client().snapshot("e1")).isNull();
    }

    @Test
    void unreachableServerGivesNull() {
        FrigateClient c = new FrigateClient("http://127.0.0.1:1", Duration.ofMillis(500), Duration.ofMillis(500));
        assertThat(c.snapshot("e1")).isNull();
    }
}

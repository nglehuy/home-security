package hsec.batch;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/** A local SparkSession in the Asia/Singapore time zone, shared by the tests of one class. */
abstract class SparkTestBase {
    static final ZoneId ZONE = ZoneId.of("Asia/Singapore");
    static final Timestamp VERSION = ts("2026-10-04 12:00:00");
    static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("hsec-batch-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.session.timeZone", ZONE.getId())
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    /** A local time in Singapore. */
    static Timestamp ts(String local) {
        return Timestamp.from(LocalDateTime.parse(local.replace(' ', 'T')).atZone(ZONE).toInstant());
    }
}

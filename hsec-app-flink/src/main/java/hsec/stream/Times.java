package hsec.stream;

import java.util.Locale;
import org.apache.flink.table.data.TimestampData;

/** Conversions for Frigate times, which are Unix seconds with a fraction. */
public final class Times {
    private Times() {}

    /** Seconds to an Iceberg timestamptz value, kept to the microsecond. */
    public static TimestampData fromSeconds(Double seconds) {
        if (seconds == null) {
            return null;
        }
        long micros = Math.round(seconds * 1_000_000d);
        return TimestampData.fromEpochMillis(Math.floorDiv(micros, 1000), (int) Math.floorMod(micros, 1000) * 1000);
    }

    public static TimestampData fromMillis(long millis) {
        return TimestampData.fromEpochMillis(millis);
    }

    /** Seconds as plain text with 6 decimals, the same form as the bridge headers. */
    public static String format(double seconds) {
        return String.format(Locale.ROOT, "%.6f", seconds);
    }

    public static long toMillis(double seconds) {
        return Math.round(seconds * 1000d);
    }
}

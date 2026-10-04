package hsec.batch;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/** Time rules of the batch job. All of them use the local time of the run. */
public final class Schedule {
    public static final LocalTime MAINTENANCE_START = LocalTime.of(3, 0);
    public static final LocalTime MAINTENANCE_END = LocalTime.of(3, 15);
    /** Tables keep the current UTC day plus the 27 days before. */
    public static final int RETENTION_DAYS = 27;

    private Schedule() {}

    /** Local midnight of yesterday, so the window is 24 to 48 hours long. */
    public static Instant windowStart(ZonedDateTime runStart) {
        return runStart.toLocalDate().minusDays(1).atStartOfDay(runStart.getZone()).toInstant();
    }

    /** The first run at or after 03:00 local time does the maintenance. Runs start every 15 minutes. */
    public static boolean maintenanceDue(ZonedDateTime runStart) {
        LocalTime t = runStart.toLocalTime();
        return !t.isBefore(MAINTENANCE_START) && t.isBefore(MAINTENANCE_END);
    }

    /** UTC midnight 27 days ago. Rows before it are deleted. */
    public static Instant retentionCutoff(Instant now) {
        return now.atZone(ZoneOffset.UTC).toLocalDate().minusDays(RETENTION_DAYS).atStartOfDay(ZoneOffset.UTC)
                .toInstant();
    }

    /** True if t is in the night window. The window can cross midnight, for example 23:00 to 06:00. */
    public static boolean isNight(LocalTime t, LocalTime start, LocalTime end) {
        if (start.isBefore(end)) {
            return !t.isBefore(start) && t.isBefore(end);
        }
        return !t.isBefore(start) || t.isBefore(end);
    }
}

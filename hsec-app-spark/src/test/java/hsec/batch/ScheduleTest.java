package hsec.batch;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class ScheduleTest {
    static final ZoneId ZONE = ZoneId.of("Asia/Singapore");

    static ZonedDateTime at(String local) {
        return LocalDateTime.parse(local).atZone(ZONE);
    }

    @Test
    void windowStartsAtLocalMidnightOfYesterday() {
        Instant midnight = at("2026-10-03T00:00").toInstant();
        assertThat(Schedule.windowStart(at("2026-10-04T00:05"))).isEqualTo(midnight);
        assertThat(Schedule.windowStart(at("2026-10-04T23:55"))).isEqualTo(midnight);
    }

    @Test
    void maintenanceRunsOnlyInTheFirstRunAfterThreeAm() {
        assertThat(Schedule.maintenanceDue(at("2026-10-04T03:00"))).isTrue();
        assertThat(Schedule.maintenanceDue(at("2026-10-04T03:14"))).isTrue();
        assertThat(Schedule.maintenanceDue(at("2026-10-04T02:59"))).isFalse();
        assertThat(Schedule.maintenanceDue(at("2026-10-04T03:15"))).isFalse();
    }

    @Test
    void retentionCutoffIsUtcMidnight27DaysAgo() {
        assertThat(Schedule.retentionCutoff(Instant.parse("2026-10-04T19:30:00Z")))
                .isEqualTo(Instant.parse("2026-09-07T00:00:00Z"));
    }

    @Test
    void nightWindowAcrossMidnight() {
        LocalTime s = LocalTime.of(23, 0);
        LocalTime e = LocalTime.of(6, 0);
        assertThat(Schedule.isNight(LocalTime.of(23, 0), s, e)).isTrue();
        assertThat(Schedule.isNight(LocalTime.of(2, 0), s, e)).isTrue();
        assertThat(Schedule.isNight(LocalTime.of(6, 0), s, e)).isFalse();
        assertThat(Schedule.isNight(LocalTime.of(12, 0), s, e)).isFalse();
    }

    @Test
    void nightWindowWithinOneDay() {
        LocalTime s = LocalTime.of(1, 0);
        LocalTime e = LocalTime.of(5, 0);
        assertThat(Schedule.isNight(LocalTime.of(0, 30), s, e)).isFalse();
        assertThat(Schedule.isNight(LocalTime.of(3, 0), s, e)).isTrue();
        assertThat(Schedule.isNight(LocalTime.of(5, 0), s, e)).isFalse();
    }
}

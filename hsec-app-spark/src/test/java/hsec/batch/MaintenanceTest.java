package hsec.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaintenanceTest {
    static final List<String> SQL = Maintenance.statements(Instant.parse("2026-10-04T19:05:00Z"));

    @Test
    void deletesWholeDaysBeforeTheCutoff() {
        assertThat(SQL).contains("DELETE FROM lake.hsec.events WHERE event_ts < TIMESTAMP '2026-09-07 00:00:00 UTC'");
    }

    @Test
    void compactsEveryTableExceptAlerts() {
        assertThat(SQL).contains("CALL lake.system.rewrite_data_files(table => 'hsec.events', strategy => 'sort')");
        assertThat(SQL).noneMatch(s -> s.contains("rewrite_data_files(table => 'hsec.alerts'"));
    }

    @Test
    void expiresSnapshotsOlderThanOneDay() {
        assertThat(SQL).contains("CALL lake.system.expire_snapshots(table => 'hsec.visits', older_than => "
                + "TIMESTAMP '2026-10-03 19:05:00 UTC', retain_last => 1)");
        assertThat(SQL).contains("CALL lake.system.remove_orphan_files(table => 'hsec.alerts', older_than => "
                + "TIMESTAMP '2026-10-03 19:05:00 UTC')");
    }

    @Test
    void coversAllSevenTablesInOrder() {
        // 7 deletes, 6 compactions, 7 expiries, 7 orphan removals, 7 manifest rewrites.
        assertThat(SQL).hasSize(34);
        assertThat(SQL.get(0)).startsWith("DELETE FROM lake.hsec.events");
        assertThat(SQL.get(SQL.size() - 1)).isEqualTo("CALL lake.system.rewrite_manifests('hsec.alerts')");
    }
}

package hsec.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * The job writes rows in the order of the schemas in Tables, and the tests make test
 * tables from them. This test makes sure that they match the migrations that create
 * the real tables in hsec-db-iceberg/migrations.
 */
class MigrationSchemaTest {
    private static final Path MIGRATIONS = Path.of("..", "hsec-db-iceberg", "migrations");
    private static final Pattern CREATE = Pattern.compile(
            "CREATE TABLE IF NOT EXISTS lake\\.hsec\\.(\\w+) \\((.*?)\\)\\s*USING iceberg\\s*"
                    + "PARTITIONED BY \\(days\\((\\w+)\\)\\)",
            Pattern.DOTALL);
    private static final Pattern ADD_COLUMN = Pattern.compile(
            "ALTER TABLE lake\\.hsec\\.(\\w+) ADD COLUMNS? \\(?\\s*(\\w+) ([\\w<>]+)");
    private static final Pattern ORDER = Pattern.compile("ALTER TABLE lake\\.hsec\\.(\\w+) WRITE ORDERED BY (\\w+), (\\w+);");

    @Test
    void javaSchemasMatchTheMigrations() throws IOException {
        Map<String, List<String>> fromSql = new LinkedHashMap<>();
        Map<String, String> partitions = new LinkedHashMap<>();
        Map<String, String> orders = new LinkedHashMap<>();
        for (Path file : upFiles()) {
            String sql = Files.readString(file);
            Matcher c = CREATE.matcher(sql);
            while (c.find()) {
                List<String> cols = new ArrayList<>();
                for (String line : c.group(2).split(",\\s*\\n")) {
                    String[] p = line.trim().split("\\s+");
                    cols.add(p[0] + " " + p[1]);
                }
                fromSql.put(c.group(1), cols);
                partitions.put(c.group(1), c.group(3));
            }
            Matcher a = ADD_COLUMN.matcher(sql);
            while (a.find()) {
                fromSql.get(a.group(1)).add(a.group(2) + " " + a.group(3));
            }
            Matcher o = ORDER.matcher(sql);
            while (o.find()) {
                orders.put(o.group(1), o.group(2) + ", " + o.group(3));
            }
        }

        assertThat(fromSql.keySet()).containsExactlyInAnyOrderElementsOf(Tables.SCHEMAS.keySet());
        for (String table : Tables.SCHEMAS.keySet()) {
            Schema s = Tables.schema(table);
            List<String> fromJava = s.columns().stream().map(f -> f.name() + " " + sqlType(f.type())).toList();
            assertThat(fromSql.get(table)).as(table).containsExactlyElementsOf(fromJava);
            assertThat(partitions.get(table)).as(table + " partition").isEqualTo("event_ts");
            String order = Tables.sortOrder(table, s).fields().stream()
                    .map(f -> s.findColumnName(f.sourceId())).reduce((x, y) -> x + ", " + y).orElseThrow();
            assertThat(orders.get(table)).as(table + " sort order").isEqualTo(order);
        }
    }

    private static List<Path> upFiles() throws IOException {
        try (Stream<Path> s = Files.list(MIGRATIONS)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".up.sql")).sorted().toList();
        }
    }

    private static String sqlType(org.apache.iceberg.types.Type t) {
        return switch (t.typeId()) {
            case STRING -> "string";
            case DOUBLE -> "double";
            case INTEGER -> "int";
            case BOOLEAN -> "boolean";
            case BINARY -> "binary";
            case TIMESTAMP -> ((Types.TimestampType) t).shouldAdjustToUTC() ? "timestamp" : "timestamp_ntz";
            case LIST -> "array<" + sqlType(t.asListType().elementType()) + ">";
            default -> throw new IllegalArgumentException("No SQL type for " + t);
        };
    }
}

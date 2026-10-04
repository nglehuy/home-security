package hsec.batch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

/** Each result of Transforms must match its table in hsec-db-clickhouse/migrations, by name and type. */
class ClickHouseSchemaTest extends SparkTestBase {
    private static final Path MIGRATIONS = Path.of("..", "hsec-db-clickhouse", "migrations");
    private static final Pattern CREATE = Pattern.compile("CREATE TABLE hsec\\.(\\w+)\\s*\\((.*?)\\)\\s*ENGINE",
            Pattern.DOTALL);

    @Test
    void transformsMatchTheClickHouseTables() throws Exception {
        Map<String, List<String>> tables = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(MIGRATIONS)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".up.sql")).sorted().toList()) {
                Matcher m = CREATE.matcher(Files.readString(f));
                while (m.find()) {
                    List<String> cols = new ArrayList<>();
                    for (String line : m.group(2).split(",\\s*\\n")) {
                        String[] p = line.trim().split("\\s+", 2);
                        cols.add(p[0] + " " + p[1].trim());
                    }
                    tables.put(m.group(1), cols);
                }
            }
        }

        Dataset<Row> objects = Transforms.objects(TransformsTest.events(), VERSION);
        Dataset<Row> visits = Transforms.visits(TransformsTest.visits(), VERSION);
        Dataset<Row> alerts = Transforms.alerts(TransformsTest.alerts(), VERSION);
        Map<String, Dataset<Row>> results = Map.of(
                "objects", objects,
                "visits", visits,
                "alerts", alerts,
                "hourly_activity", Transforms.hourlyActivity(objects, VERSION),
                "daily_stats", Transforms.dailyStats(objects, visits, alerts, LocalTime.of(23, 0),
                        LocalTime.of(6, 0), VERSION));

        assertThat(tables.keySet()).containsExactlyInAnyOrderElementsOf(results.keySet());
        for (var e : results.entrySet()) {
            List<String> spark = new ArrayList<>();
            for (StructField f : e.getValue().schema().fields()) {
                spark.add(f.name() + " " + clickHouseType(f.dataType()));
            }
            List<String> expected = tables.get(e.getKey()).stream().map(ClickHouseSchemaTest::plain).toList();
            assertThat(spark).as(e.getKey()).containsExactlyElementsOf(expected);
        }
    }

    /** ClickHouse type without Nullable and LowCardinality, which do not change the Spark type. */
    private static String plain(String col) {
        return col.replaceAll("LowCardinality\\((\\w+)\\)", "$1").replaceAll("Nullable\\((.+)\\)", "$1")
                .replace("DateTime64(3)", "DateTime");
    }

    private static String clickHouseType(DataType t) {
        if (t.equals(DataTypes.StringType)) {
            return "String";
        } else if (t.equals(DataTypes.BooleanType)) {
            return "Bool";
        } else if (t.equals(DataTypes.FloatType)) {
            return "Float32";
        } else if (t.equals(DataTypes.IntegerType)) {
            return "Int32";
        } else if (t.equals(DataTypes.TimestampType)) {
            return "DateTime";
        } else if (t.equals(DataTypes.DateType)) {
            return "Date";
        } else if (t instanceof ArrayType a) {
            return "Array(" + clickHouseType(a.elementType()) + ")";
        }
        throw new IllegalArgumentException("No ClickHouse type for " + t);
    }
}

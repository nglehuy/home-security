package hsec.stream;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VisitFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<String, EventMsg, Visit> h;

    @BeforeEach
    void setUp() throws Exception {
        h = ProcessFunctionTestHarnesses.forKeyedProcessFunction(new VisitFunction(), e -> e.camera, Types.STRING);
        h.setProcessingTime(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        h.close();
    }

    private static EventMsg person(String type, String id, double start, Double end, String name, String... zones) {
        EventMsg e = new EventMsg();
        e.type = type;
        e.id = id;
        e.camera = "front";
        e.label = "person";
        e.subLabel = name;
        e.startTime = start;
        e.frameTime = end != null ? end : start;
        e.endTime = end;
        e.enteredZones = zones;
        return e;
    }

    private void at(long minutesTimesTen, EventMsg e) throws Exception {
        h.setProcessingTime(minutesTimesTen);
        h.processElement(e, 0);
    }

    @Test
    void peopleThreeMinutesApartAreOneVisit() throws Exception {
        at(0, person("new", "a", 1000, null, null, "yard"));
        at(60_000, person("end", "a", 1000, 1060.0, null));
        at(240_000, person("new", "b", 1240, null, "John", "porch"));
        at(300_000, person("end", "b", 1240, 1300.0, "John"));
        h.setProcessingTime(360_000);
        assertThat(h.extractOutputValues()).isEmpty();

        h.setProcessingTime(600_000);
        assertThat(h.extractOutputValues()).hasSize(1);
        Visit v = h.extractOutputValues().get(0);
        assertThat(v.visitId).isEqualTo("front:1000000");
        assertThat(v.startTime).isEqualTo(1000);
        assertThat(v.endTime).isEqualTo(1300);
        assertThat(v.personCount).isEqualTo(2);
        assertThat(v.knownNames).containsExactly("John");
        assertThat(v.unknownCount).isEqualTo(1);
        assertThat(v.zones).containsExactly("porch", "yard");
    }

    @Test
    void peopleSixMinutesApartAreTwoVisits() throws Exception {
        at(0, person("new", "a", 1000, null, null));
        at(60_000, person("end", "a", 1000, 1060.0, null));
        h.setProcessingTime(360_000);
        at(420_000, person("new", "b", 1420, null, null));
        at(480_000, person("end", "b", 1420, 1480.0, null));
        h.setProcessingTime(780_000);
        assertThat(h.extractOutputValues()).hasSize(2);
        assertThat(h.extractOutputValues()).extracting(v -> v.personCount).containsExactly(1, 1);
        assertThat(h.extractOutputValues()).extracting(v -> v.visitId).containsExactly("front:1000000", "front:1420000");
    }

    @Test
    void visitStaysOpenWhileSomeoneIsStillInView() throws Exception {
        at(0, person("new", "a", 1000, null, null));
        at(0, person("new", "b", 1000, null, null));
        at(60_000, person("end", "a", 1000, 1060.0, null));
        h.setProcessingTime(1_000_000);
        assertThat(h.extractOutputValues()).isEmpty();
    }

    @Test
    void nameThatArrivesLaterCounts() throws Exception {
        at(0, person("new", "a", 1000, null, null));
        at(10_000, person("update", "a", 1000, null, "Ann"));
        at(20_000, person("end", "a", 1000, 1020.0, null));
        h.setProcessingTime(320_000);
        Visit v = h.extractOutputValues().get(0);
        assertThat(v.knownNames).containsExactly("Ann");
        assertThat(v.unknownCount).isZero();
    }

    @Test
    void ignoresOtherLabels() throws Exception {
        EventMsg car = person("new", "c", 1000, null, null);
        car.label = "car";
        at(0, car);
        car.type = "end";
        at(10_000, car);
        h.setProcessingTime(1_000_000);
        assertThat(h.extractOutputValues()).isEmpty();
    }
}

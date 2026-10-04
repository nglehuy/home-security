package hsec.stream;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AlertFunctionTest {
    private KeyedTwoInputStreamOperatorTestHarness<String, EventMsg, ReviewMsg, AlertRequest> h;

    @BeforeEach
    void setUp() throws Exception {
        h = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new AlertFunction(), e -> e.camera, r -> r.camera, Types.STRING);
        h.setProcessingTime(seconds(2000));
    }

    @AfterEach
    void tearDown() throws Exception {
        h.close();
    }

    private static long seconds(double s) {
        return Math.round(s * 1000);
    }

    private static EventMsg event(String id, String label, double topScore) {
        EventMsg e = new EventMsg();
        e.type = "new";
        e.id = id;
        e.camera = "front";
        e.label = label;
        e.topScore = topScore;
        return e;
    }

    private static ReviewMsg review(String type, String severity, double start, Double end, String... detections) {
        ReviewMsg r = new ReviewMsg();
        r.type = type;
        r.id = "r1";
        r.camera = "front";
        r.severity = severity;
        r.startTime = start;
        r.endTime = end;
        r.detections = detections;
        r.objects = new String[] {"person"};
        r.subLabels = new String[0];
        r.zones = new String[] {"yard"};
        return r;
    }

    private List<AlertRequest> out() {
        return h.extractOutputValues();
    }

    @Test
    void newAlertReviewGivesOneStartRequest() throws Exception {
        h.processElement2(review("new", "alert", 1000, null, "e1"), 0);
        assertThat(out()).hasSize(1);
        AlertRequest a = out().get(0);
        assertThat(a.kind).isEqualTo(AlertRequest.START);
        assertThat(a.alertId).isEqualTo("r1:start");
        assertThat(a.eventId).isEqualTo("e1");
        assertThat(a.from).isEqualTo(1000);
        assertThat(a.to).isEqualTo(1000);
        assertThat(a.zones).containsExactly("yard");
        assertThat(a.firedMs).isEqualTo(seconds(2000));
    }

    @Test
    void detectionThatBecomesAnAlertGivesOneStartRequest() throws Exception {
        h.processElement2(review("new", "detection", 1000, null, "e1"), 0);
        assertThat(out()).isEmpty();
        h.processElement2(review("update", "alert", 1000, null, "e1"), 0);
        assertThat(out()).hasSize(1);
    }

    @Test
    void secondAlertMessageForTheSameReviewGivesNothing() throws Exception {
        h.processElement2(review("new", "alert", 1000, null, "e1"), 0);
        h.processElement2(review("update", "alert", 1000, null, "e1", "e2"), 0);
        assertThat(out()).hasSize(1);
    }

    @Test
    void reviewThatStaysADetectionGivesNothing() throws Exception {
        h.processElement2(review("new", "detection", 1000, null, "e1"), 0);
        h.processElement2(review("end", "detection", 1000, 1060.0, "e1"), 0);
        assertThat(out()).isEmpty();
    }

    @Test
    void picksThePersonWithTheHighestScore() throws Exception {
        h.processElement1(event("car1", "car", 0.99), 0);
        h.processElement1(event("p1", "person", 0.70), 0);
        h.processElement1(event("p2", "person", 0.90), 0);
        h.processElement2(review("new", "alert", 1000, null, "car1", "p1", "p2"), 0);
        assertThat(out().get(0).eventId).isEqualTo("p2");
    }

    @Test
    void picksTheFirstIdWhenNoPersonIsKnown() throws Exception {
        h.processElement2(review("new", "alert", 1000, null, "x1", "x2"), 0);
        assertThat(out().get(0).eventId).isEqualTo("x1");
    }

    @Test
    void reviewWithoutDetectionsStillSendsMessageOne() throws Exception {
        h.processElement2(review("new", "alert", 1000, null), 0);
        assertThat(out()).hasSize(1);
        assertThat(out().get(0).eventId).isNull();
    }

    @Test
    void sixtySecondReviewGivesThreeWindows() {
        ReviewMsg r = review("end", "alert", 1000, 1060.0, "e1");
        List<AlertRequest> c = AlertFunction.clips(r, 0);
        assertThat(c).extracting(x -> x.from).containsExactly(1000.0, 1025.0, 1050.0);
        assertThat(c).extracting(x -> x.to).containsExactly(1025.0, 1050.0, 1060.0);
        assertThat(c).extracting(x -> x.part).containsExactly(1, 2, 3);
        assertThat(c).allMatch(x -> x.partCount == 3);
        assertThat(c).extracting(x -> x.alertId).containsExactly("r1:clip:1", "r1:clip:2", "r1:clip:3");
        assertThat(c).extracting(x -> x.readyMs).containsExactly(seconds(1045), seconds(1070), seconds(1080));
    }

    @Test
    void endMessageSendsReadyWindowsAtOnceAndTheOthersLater() throws Exception {
        h.setProcessingTime(seconds(1075));
        h.processElement2(review("end", "alert", 1000, 1060.0, "e1"), 0);
        // Message 1, then the two windows that ended more than 20 seconds ago.
        assertThat(out()).extracting(x -> x.alertId).containsExactly("r1:start", "r1:clip:1", "r1:clip:2");

        h.setProcessingTime(seconds(1079));
        assertThat(out()).hasSize(3);
        h.setProcessingTime(seconds(1080));
        assertThat(out()).extracting(x -> x.alertId).containsExactly("r1:start", "r1:clip:1", "r1:clip:2", "r1:clip:3");
    }

    @Test
    void reviewWithEqualStartAndEndGivesOneWindow() {
        List<AlertRequest> c = AlertFunction.clips(review("end", "alert", 1000, 1000.0), 0);
        assertThat(c).hasSize(1);
        assertThat(c.get(0).from).isEqualTo(1000);
        assertThat(c.get(0).to).isEqualTo(1000);
        assertThat(c.get(0).partCount).isEqualTo(1);
    }

    @Test
    void endWithoutEndTimeGivesOneWindow() {
        List<AlertRequest> c = AlertFunction.clips(review("end", "alert", 1000, null), 0);
        assertThat(c).hasSize(1);
    }

    @Test
    void waitingWindowsSurviveARestart() throws Exception {
        h.setProcessingTime(seconds(1061));
        h.processElement2(review("end", "alert", 1000, 1060.0, "e1"), 0);
        // Window 1 was ready at 1045, so only windows 2 and 3 wait for their timers.
        assertThat(out()).extracting(x -> x.alertId).containsExactly("r1:start", "r1:clip:1");
        var snapshot = h.snapshot(1, 1);
        h.close();

        h = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(new AlertFunction()), e -> e.camera, r -> r.camera, Types.STRING);
        h.setup();
        h.initializeState(snapshot);
        h.open();
        h.setProcessingTime(seconds(1080));
        assertThat(out()).extracting(x -> x.alertId).containsExactly("r1:clip:2", "r1:clip:3");
    }
}

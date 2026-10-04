package hsec.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class DedupeTest {
    private static Envelope env(String topic, String body) {
        return Parser.parse(Samples.raw(topic, body), 0).orElseThrow();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Envelope, Envelope> harness() throws Exception {
        return ProcessFunctionTestHarnesses.forKeyedProcessFunction(new Dedupe(), e -> e.messageId, Types.STRING);
    }

    @Test
    void sameMessageTwiceGivesOneRow() throws Exception {
        try (var h = harness()) {
            h.processElement(env("frigate.available", "online"), 0);
            h.processElement(env("frigate.available", "online"), 1);
            assertThat(h.extractOutputValues()).hasSize(1);
        }
    }

    @Test
    void twoDifferentMessagesGiveTwoRows() throws Exception {
        try (var h = harness()) {
            h.processElement(env("frigate.available", "online"), 0);
            h.processElement(env("frigate.available", "offline"), 1);
            assertThat(h.extractOutputValues()).hasSize(2);
        }
    }

    @Test
    void sameBodyOnTwoTopicsGivesTwoRows() throws Exception {
        try (var h = harness()) {
            h.processElement(env("frigate.available", "online"), 0);
            h.processElement(env("frigate.restart", "online"), 1);
            assertThat(h.extractOutputValues()).hasSize(2);
        }
    }

    @Test
    void forgetsAMessageAfterTenMinutes() throws Exception {
        try (var h = harness()) {
            h.setStateTtlProcessingTime(0);
            h.processElement(env("frigate.available", "online"), 0);
            h.setStateTtlProcessingTime(Dedupe.TTL.toMillis() + 1);
            h.processElement(env("frigate.available", "online"), 0);
            assertThat(h.extractOutputValues()).hasSize(2);
        }
    }
}

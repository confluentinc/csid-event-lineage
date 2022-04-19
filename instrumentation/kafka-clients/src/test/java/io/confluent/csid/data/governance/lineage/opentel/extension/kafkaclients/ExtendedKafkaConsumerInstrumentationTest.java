/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;


import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.CAPTURE_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.CHARSET_UTF_8;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.NOT_CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.NOT_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.PROPAGATION_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.TraceAssertData.trace;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class ExtendedKafkaConsumerInstrumentationTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;
  private String testTopic2;

  private CommonTestUtils commonTestUtils;

  @BeforeAll
  static void setupAll() {
    setupHeaderConfiguration();
  }

  @AfterAll
  static void cleanupAll() {
    cleanupHeaderConfiguration();
  }

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
    testTopic = "test-topic-" + UUID.randomUUID();
    testTopic2 = "test-topic-2-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }

  @Test
  @DisplayName("Test consumer process span records a Trace with consume span")
  void testConsumerProcessSpanSimple() {
    String key = "key";
    String value = "value";

    commonTestUtils.produceSingleEvent(testTopic, key, value);
    commonTestUtils.consumeEvent(testTopic);
    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    assertTracesCaptured(traces, trace().withSpans(produce(), consume()));
  }

  @SneakyThrows
  @Test
  @DisplayName("Test consumer process span records a Trace with consume span and captures only whitelisted headers")
  void testConsumerProcessSpanWithWhitelistedHeaderCapturePartial() {
    String key = "key";
    String value = "value";

    Header[] sentMessageHeaders = ArrayUtils.addAll(CAPTURE_WHITELISTED_HEADERS,
        NOT_WHITELISTED_HEADERS);

    commonTestUtils.produceSingleEvent(testTopic, key, value, sentMessageHeaders);
    commonTestUtils.consumeEvent(testTopic);

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    assertTracesCaptured(traces,
        trace().withSpans(produce(),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)));
  }

  @SneakyThrows
  @Test
  @DisplayName("Test consumer to producer header propagation")
  void testHeaderPropagationFromConsumerToProducer() {

    String key = "key";
    String value = "value";

    Header[] sentMessageHeaders = ArrayUtils.addAll(PROPAGATION_WHITELISTED_HEADERS,
        NOT_WHITELISTED_HEADERS);
    commonTestUtils.produceSingleEvent(testTopic, key, value, sentMessageHeaders);
    commonTestUtils.consumeEvents(testTopic, 1,
        record -> commonTestUtils.produceSingleEvent(testTopic2, key, value));

    ConsumerRecord<String, String> consumedRecord = commonTestUtils.consumeEvent(testTopic2);
    List<List<SpanData>> traces = instrumentation.waitForTraces(1);

    assertTracesCaptured(traces,
        trace().withSpans(
            produce(),
            consume(),
            produce().withNameContaining("test-topic-2-")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_CAPTURED_PROPAGATED_HEADER),
            consume().withNameContaining("test-topic-2-")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER)));
    assertThat(consumedRecord.headers()).contains(PROPAGATION_WHITELISTED_HEADERS);
  }
}

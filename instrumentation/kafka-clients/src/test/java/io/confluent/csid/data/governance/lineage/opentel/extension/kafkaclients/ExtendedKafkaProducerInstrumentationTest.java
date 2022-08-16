/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.CAPTURE_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.CHARSET_UTF_8;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.NOT_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.TraceAssertData.trace;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ExtendedKafkaProducerInstrumentationTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;
  private CommonTestUtils commonTestUtils;

  @BeforeAll
  static void setupAll() {
    setupHeaderConfiguration(Singletons.headerCaptureConfiguration());
  }

  @AfterAll
  static void cleanupAll() {
    cleanupHeaderConfiguration(Singletons.headerCaptureConfiguration());
  }

  @BeforeEach
  void setup() {
    testTopic = "test-topic-" + UUID.randomUUID();
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
  }

  @AfterEach
  void teardown() {
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }


  @SneakyThrows
  @Test
  @DisplayName("Test producer send span records a trace with Produce span")
  void testProducerSpanSimple() {
    String key = "key";
    String value = "value";
    commonTestUtils.produceSingleEvent(testTopic, key, value);

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    assertTracesCaptured(traces, trace().withSpans(produce()));
  }

  @Test
  @DisplayName("Test producer send span records a trace with Produce span and captures whitelisted headers")
  void testProducerSpanWithHeaderCapture() {

    String key = "key";
    String value = "value";

    commonTestUtils.produceSingleEvent(testTopic, key, value, CAPTURE_WHITELISTED_HEADERS);

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    assertTracesCaptured(traces,
        trace().withSpans(produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)));
  }

  @Test
  @DisplayName("Test producer send span records a trace with Produce span and captures only whitelisted headers")
  void testProducerSpanWithHeaderCapturePartial() {
    String key = "key";
    String value = "value";

    Header[] headers = ArrayUtils.addAll(CAPTURE_WHITELISTED_HEADERS,
        NOT_WHITELISTED_HEADERS);

    commonTestUtils.produceSingleEvent(testTopic, key, value, headers);

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    assertTracesCaptured(traces, trace().withSpans(
        produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
            .withoutHeaders(NOT_WHITELISTED_HEADERS)));
  }
}

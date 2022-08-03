/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.smt;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TraceAssertData.trace;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.ConnectStandalone;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class SmtTracingTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;
  private final Charset charset = StandardCharsets.UTF_8;
  private final String transformClassName = "InsertHeaderBytes";
  CommonTestUtils commonTestUtils;

  @BeforeAll
  public static void setupAll() {
    setupHeaderConfiguration();
  }

  @BeforeEach
  void setup() {
    testTopic = "test-topic-" + UUID.randomUUID();
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
  }

  @AfterEach
  void cleanup() {
    instrumentation.clearData();
    commonTestUtils.stopKafkaContainer();
  }

  @AfterAll
  public static void cleanupAll() {
    cleanupHeaderConfiguration();
  }

  @SneakyThrows
  @Test
  void testSMTCaptureWithHeaderCapture() {
    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(null),
        commonTestUtils.getSourceTaskProperties(null, testTopic));
    CountDownLatch connectLatch = new CountDownLatch(1);
    new Thread(() -> {
      connectStandalone.start();
      try {

        connectLatch.await();
      } catch (InterruptedException e) {
      } finally {
        connectStandalone.stop();
      }
    }).start();

    commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
        testTopic, 1);

    connectLatch.countDown();

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);
    // Only checking first trace containing only SMT span - as the source task is not wired up yet
    // for tracing, so it produces a separate trace
    assertTracesCaptured(Collections.singletonList(traces.get(0)),
        trace().withSpans(smt().withNameContaining(transformClassName)
            .withHeaders(charset, CAPTURED_PROPAGATED_HEADER)));
  }
}

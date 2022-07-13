/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.smt;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.ConnectStandalone;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import org.junit.jupiter.api.io.TempDir;

public class SmtTracingTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;
  private final Charset charset = StandardCharsets.UTF_8;
  private final String transformClassName = "InsertHeaderBytes";
  CommonTestUtils commonTestUtils;
  @TempDir
  File tempDir;

  @BeforeAll
  public static void setupAll() {
    setupHeaderConfiguration();
  }

  @BeforeEach
  void setup() {
    testTopic = "test-topic-" + UUID.randomUUID();
    commonTestUtils = new CommonTestUtils(tempDir);
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
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSourceTaskProperties(
            commonTestUtils.getHeaderInjectTrasnformProperties(), testTopic));
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
    connectStandalone.awaitStop();

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    // Only checking first trace's second span - should be the SMT span.
    // Now that SourceTask is wired - first is Source Task span, followed by SMT and Producer Send.
    assertSpan(traces.get(0).get(1), smt().withNameContaining(transformClassName)
        .withHeaders(charset, CAPTURED_PROPAGATED_HEADER));
  }

  private void assertSpan(SpanData actual, SpanAssertData expectations) {
    expectations.accept(OpenTelemetryAssertions.assertThat(actual));
  }
}

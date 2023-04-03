/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.sinkTask;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TestConstants.DISABLE_PROPAGATION_UT_TAG;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TraceAssertData.trace;
import static org.awaitility.Awaitility.await;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.ConnectStandalone;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
public class SinkTaskTracingTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;
  private final Charset charset = StandardCharsets.UTF_8;
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
    commonTestUtils = new CommonTestUtils(tempDir.getAbsolutePath());
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
  void testSinkTaskCaptureWithHeaderPropagationAndCapture() {
    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSinkTaskProperties(null, testTopic));

    connectStandalone.start();

    await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).until(
        connectStandalone::isRunning);

    String key = " {\"schema\":{\"type\":\"int32\",\"optional\":false},\"payload\":0}";
    String value = "{\"schema\":{\"type\":\"int64\",\"optional\":false},\"payload\":31}";

    commonTestUtils.produceSingleEvent(testTopic, key, value, CAPTURED_PROPAGATED_HEADER);

    commonTestUtils.waitUntil("Wait for traces", () -> instrumentation.waitForTraces(1).get(0).size() == 3);

    connectStandalone.stop();

    commonTestUtils.waitUntil("Wait for traces",
        () -> instrumentation.waitForTraces(1).get(0).size() == 3);
    log.info("Header literal " + commonTestUtils.getHeaderInjectTrasnformProperties().getProperty("transforms.insertHeader.value.literal"));
    log.info("Header " + commonTestUtils.getHeaderInjectTrasnformProperties().getProperty("transforms.insertHeader.header"));

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    //Expected trace - producer send, consumer process, sink-task
    assertTracesCaptured(traces,
        trace().withSpans(produce(), consume(), sinkTask().withNameContaining(testTopic)
            .withHeaders(charset, CAPTURED_PROPAGATED_HEADER)));
  }

  /**
   * Test scenario when consumed message has no tracing context header.
   *
   * @see TestConstants.DISABLE_PROPAGATION_UT_TAG
   */
  @SneakyThrows
  @Test
  @Tag(DISABLE_PROPAGATION_UT_TAG)
  //this test passes
  void testSinkTaskCaptureWithHeaderPropagationAndCaptureWhenInboundMessageHasNoTrace() {
    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSinkTaskProperties(null, testTopic));
    connectStandalone.start();

    await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).until(
        connectStandalone::isRunning);

    String key = " {\"schema\":{\"type\":\"int32\",\"optional\":false},\"payload\":0}";
    String value = "{\"schema\":{\"type\":\"int64\",\"optional\":false},\"payload\":31}";

    commonTestUtils.produceSingleEvent(testTopic, key, value, CAPTURED_PROPAGATED_HEADER);

    commonTestUtils.waitUntil("Wait for traces", () -> instrumentation.waitForTraces(2).get(1).size() == 2);

    connectStandalone.stop();

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);
    //Expected trace - producer send, consumer process, sink-task
    assertTracesCaptured(traces,
        trace().withSpans(produce()),
        trace().withSpans(consume(), sinkTask().withNameContaining(testTopic)
            .withHeaders(charset, CAPTURED_PROPAGATED_HEADER)));
  }
}

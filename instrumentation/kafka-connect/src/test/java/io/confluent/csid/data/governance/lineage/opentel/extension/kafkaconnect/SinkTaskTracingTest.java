/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils.assertAnyTraceSatisfies;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.sinkTask;
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
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

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
  void testSinkTaskCaptureWithHeaderPropagationAndCapture() {
    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSinkTaskProperties(null, testTopic));
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

    await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).until(
        connectStandalone::isRunning);

    String key = " {\"schema\":{\"type\":\"int32\",\"optional\":false},\"payload\":0}";
    String value = "{\"schema\":{\"type\":\"int64\",\"optional\":false},\"payload\":31}";
    commonTestUtils.produceSingleEvent(testTopic, key, value, CAPTURED_PROPAGATED_HEADER);

    await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
        .until(() -> instrumentation.waitForTraces(1).get(0).size() == 3);

    connectLatch.countDown();
    connectStandalone.awaitStop();

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    //Expected trace - producer send, consumer process, sink-task
    assertAnyTraceSatisfies(traces,
        trace().withSpans(produce(), consume(), sinkTask().withNameContaining(testTopic)
            .withHeaders(charset, CAPTURED_PROPAGATED_HEADER)));
  }
}

/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.sourceTask;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.testSourcePollTask;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TraceAssertData.trace;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.ConnectStandalone;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.VerifiableSourceConnector;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.VerifiableSourceIndividuallyTracedConnector;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class SourceTaskTracingTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;
  CommonTestUtils commonTestUtils;

  @TempDir
  File tempDir;

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

  @SneakyThrows
  @Test
  void testSourceTask() {
    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSourceTaskProperties(null, testTopic, VerifiableSourceConnector.class));
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
        testTopic, 2);

    connectLatch.countDown();
    connectStandalone.awaitStop();

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);
    //Expected trace - source-task, producer send, consumer process.
    assertTracesCaptured(traces,
        trace().withSpans(sourceTask().withNameContaining(testTopic), produce(), consume()),
        trace().withSpans(sourceTask().withNameContaining(testTopic), produce(), consume()));
  }


  @SneakyThrows
  @Test
  void testSourceTaskInheritsSpanFromPoll() {
    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSourceTaskProperties(null, testTopic,
            VerifiableSourceIndividuallyTracedConnector.class));
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
        testTopic, 2);

    connectLatch.countDown();
    connectStandalone.awaitStop();

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);
    //Expected trace - test-source-poll, source-task, producer send, consumer process.
    assertTracesCaptured(traces,
        trace().withSpans(testSourcePollTask(), sourceTask().withNameContaining(testTopic),
            produce(), consume()),
        trace().withSpans(testSourcePollTask(), sourceTask().withNameContaining(testTopic),
            produce(), consume()));
  }
}

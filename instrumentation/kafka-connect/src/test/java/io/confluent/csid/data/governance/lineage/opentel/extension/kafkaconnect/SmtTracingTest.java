/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData.smt;
import static org.awaitility.Awaitility.await;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.CommonTestUtils;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.ConnectStandalone;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.SpanAssertData;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.VerifiableSourceConnector;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
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
  void testSMTCaptureWithHeaderCaptureUsedWithSourceTask() {

    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSourceTaskProperties(
            commonTestUtils.getHeaderInjectTrasnformProperties(), testTopic,
            VerifiableSourceConnector.class));
    connectStandalone.start();

    commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
        testTopic, 1);

    connectStandalone.stop();

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    log.debug("Header literal " + commonTestUtils.getHeaderInjectTrasnformProperties().getProperty("transforms.insertHeader.value.literal"));
    log.debug("Header " + commonTestUtils.getHeaderInjectTrasnformProperties().getProperty("transforms.insertHeader.header"));

    // Only checking first trace's second span - should be the SMT span.
    // Now that SourceTask is wired - first is Source Task span, followed by SMT and Producer Send.
    assertSpan(traces.get(0).get(1), smt().withNameContaining(transformClassName)
        .withHeaders(charset, CAPTURED_PROPAGATED_HEADER));
  }

  @SneakyThrows
  @Test
  void testSMTCaptureWithHeaderCaptureUsedWithSinkTask() {

    ConnectStandalone connectStandalone = new ConnectStandalone(
        commonTestUtils.getConnectWorkerProperties(),
        commonTestUtils.getSinkTaskProperties(
            commonTestUtils.getHeaderInjectTrasnformProperties(), testTopic));
    connectStandalone.start();

    log.debug("Header literal pt1 " + commonTestUtils.getHeaderInjectTrasnformProperties().getProperty("transforms.insertHeader.value.literal"));

    await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).until(
        connectStandalone::isRunning);

    String key = " {\"schema\":{\"type\":\"int32\",\"optional\":false},\"payload\":0}";
    String value = "{\"schema\":{\"type\":\"int64\",\"optional\":false},\"payload\":31}";
    commonTestUtils.produceSingleEvent(testTopic, key, value);

    commonTestUtils.waitUntil("Wait for traces", () -> instrumentation.waitForTraces(1).get(0).size() == 4);

    connectStandalone.stop();
    commonTestUtils.waitUntil("Wait for traces", () -> instrumentation.waitForTraces(1).get(0).get(2).getAttributes().size() == 4);


    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    log.debug("Header literal pt2" + commonTestUtils.getHeaderInjectTrasnformProperties().getProperty("transforms.insertHeader.value.literal"));
    log.debug("Header " + commonTestUtils.getHeaderInjectTrasnformProperties().getProperty("transforms.insertHeader.header"));

    // Only checking first trace's third span - should be the SMT span.
    // Now that SinkTask is wired - first is producer send span, followed by consumer process, SMT and Sink task.
    assertSpan(traces.get(0).get(2), smt().withNameContaining(transformClassName)
        .withHeaders(charset, CAPTURED_PROPAGATED_HEADER));
  }

  private void assertSpan(SpanData actual, SpanAssertData expectations) {
    expectations.accept(OpenTelemetryAssertions.assertThat(actual));
  }
}

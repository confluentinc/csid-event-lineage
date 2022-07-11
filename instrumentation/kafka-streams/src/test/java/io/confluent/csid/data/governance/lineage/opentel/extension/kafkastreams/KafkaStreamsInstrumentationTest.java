/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURE_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.NOT_CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.NOT_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreGet;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStorePut;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.TraceAssertData.trace;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class KafkaStreamsInstrumentationTest {

  @BeforeAll
  static void setupAll() {
    setupHeaderConfiguration();
  }

  @AfterAll
  static void cleanupAll() {
    cleanupHeaderConfiguration();
  }

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String inputTopic;
  private String outputTopic;
  private CommonTestUtils commonTestUtils;

  @TempDir
  File tempDir;

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils(tempDir);
    inputTopic = "input-topic-" + UUID.randomUUID();
    outputTopic = "output-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    instrumentation.clearData();
  }

  @Test
  @DisplayName("Test KStream header capture with state store operation (GroupByKey -> count) using topology test driver")
  void testHeaderCaptureWithStateStoreOperation() {

    String key = "";
    String value = "";
    Charset charset = StandardCharsets.UTF_8;

    Headers recordHeaders = new RecordHeaders();
    Arrays.stream(CAPTURE_WHITELISTED_HEADERS).forEach(recordHeaders::add);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.<String, String>stream(inputTopic).groupByKey().count().toStream()
        .to(outputTopic);
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(),
        commonTestUtils.getPropertiesForStreams());
    TestInputTopic<String, String> testInputTopic = topologyTestDriver.createInputTopic(inputTopic,
        Serdes.String().serializer(),
        Serdes.String().serializer());
    TestOutputTopic<String, Long> testOutputTopic = topologyTestDriver.createOutputTopic(
        outputTopic, Serdes.String().deserializer(),
        Serdes.Long().deserializer());
    testInputTopic.pipeInput(new TestRecord<>(key, value, recordHeaders));

    await().atMost(Duration.ofSeconds(5)).until(() -> !testOutputTopic.isEmpty());

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);

    assertTracesCaptured(traces,
        trace().withSpans(
            consume().withHeaders(charset, CAPTURE_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(charset, CAPTURE_WHITELISTED_HEADERS)
        ));
  }

  @Test
  @DisplayName("Test KStream header propagation over state store operation (GroupByKey -> count) using topology test driver")
  void testHeaderPropagationOverStateStore() {
    Charset charset = StandardCharsets.UTF_8;

    String key = "k";
    String value = "v";

    Header[] allHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    Headers allRecordHeaders = new RecordHeaders();
    Arrays.stream(allHeaders).forEach(allRecordHeaders::add);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.<String, String>stream(inputTopic).groupByKey().count().toStream()
        .to(outputTopic);
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(),
        commonTestUtils.getPropertiesForStreams());
    TestInputTopic<String, String> testInputTopic = topologyTestDriver.createInputTopic(inputTopic,
        Serdes.String().serializer(),
        Serdes.String().serializer());
    TestOutputTopic<String, Long> testOutputTopic = topologyTestDriver.createOutputTopic(
        outputTopic, Serdes.String().deserializer(),
        Serdes.Long().deserializer());
    testInputTopic.pipeInput(new TestRecord<>(key, value, allRecordHeaders));
    testInputTopic.pipeInput(new TestRecord<>(key, value));

    await().atMost(Duration.ofSeconds(5)).until(() -> !testOutputTopic.isEmpty());

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);

    assertTracesCaptured(traces,
        trace().withSpans(
            consume().withHeaders(charset, CAPTURE_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(charset, CAPTURE_WHITELISTED_HEADERS)
        ),
        trace().withSpans(consume().withoutHeaders(allHeaders),
            stateStoreGet().withHeaders(charset, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_CAPTURED_PROPAGATED_HEADER.key()),
            stateStorePut().withHeaders(charset, CAPTURED_PROPAGATED_HEADER)
                .withoutHeaders(NOT_CAPTURED_PROPAGATED_HEADER.key())));
  }
}

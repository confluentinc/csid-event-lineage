/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CHARSET_UTF_8;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.produceChangelog;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreGet;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStorePut;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.TraceAssertData.trace;

import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for tracing propagation during Stream to KTable join operations that utilize StateStore
 * put/get.
 */
public class KafkaStreamsStateStoreInstrumentationKTableJoinTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String inputTopic;
  private String ktableTopic;
  private String outputTopic;

  private CommonTestUtils commonTestUtils;
  private final CountDownLatch streamsLatch = new CountDownLatch(1);

  private KafkaStreams kafkaStreams;

  @TempDir
  File tempDir;

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
    commonTestUtils = new CommonTestUtils(tempDir);
    commonTestUtils.startKafkaContainer();
    inputTopic = "input-topic-" + UUID.randomUUID();
    ktableTopic = "ktable-topic-" + UUID.randomUUID();
    outputTopic = "output-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    streamsLatch.countDown();
    commonTestUtils.awaitKStreamsShutdown(kafkaStreams);
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }


  @Test
  @DisplayName("Test KStream tracing, header capture and propagation with KTable join state store operation")
  void testKStreamTracingWithKTableJoinOperation() {
    String key = "key";
    String value = "value";

    kafkaStreams = prepareKStreamTopologyWithKTable();
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic, outputTopic, ktableTopic);

    commonTestUtils.produceSingleEvent(ktableTopic, key, value, CAPTURED_PROPAGATED_HEADER);
    commonTestUtils.produceSingleEvent(inputTopic, key, value);

    commonTestUtils.consumeEvent(Serdes.String().deserializer().getClass(),
        Serdes.String().deserializer().getClass(), outputTopic);

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withNameContaining("ktable")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER),
            consume().withNameContaining("ktable")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER),
            stateStorePut().withNameContaining("ktable")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER),
            produceChangelog().withNameContaining("ktable")),
        trace().withSpans(
            produce().withNameContaining("input").withoutHeaders(CAPTURED_PROPAGATED_HEADER),
            consume().withNameContaining("input").withoutHeaders(CAPTURED_PROPAGATED_HEADER),
            stateStoreGet().withLink().withNameContaining("ktable")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER),
            produce().withNameContaining("output")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER),
            consume().withNameContaining("output")
                .withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADER)));

  }

  private KafkaStreams prepareKStreamTopologyWithKTable() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KTable<String, String> table = streamsBuilder.table(ktableTopic,
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.with(Serdes.String(), Serdes.String()));
    streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues(x -> x)
        .join(table, (value1, value2) -> value2)
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    return new KafkaStreams(streamsBuilder.build(), commonTestUtils.getPropertiesForStreams());
  }
}

/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURE_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CHARSET_UTF_8;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.NOT_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.produceChangelog;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreDelete;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreGet;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStorePut;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.TraceAssertData.trace;

import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for tracing propagation during Stream aggregation operations that produce KTable and
 * utilize StateStore put/get . Using groupByKey().count() and groupByKey().aggregate().
 */
public class KafkaStreamsStateStoreInstrumentationAggregationTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String inputTopic;
  private String outputTopic;

  private CommonTestUtils commonTestUtils;

  private CountDownLatch streamsLatch = new CountDownLatch(1);
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
  @DisplayName("Test KStream header capture with state store operation (GroupByKey -> Aggregate)")
  void testKStreamStateStoreHeaderCaptureWithAggregateOp() {

    String key = "key";
    String value = "value";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String())).groupByKey()
        .aggregate(() -> "", (k, v, c) -> c + "," + v.charAt(0),
            Materialized.with(Serdes.String(), Serdes.String()))
        .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    kafkaStreams = new KafkaStreams(streamsBuilder.build(),
        commonTestUtils.getPropertiesForStreams());
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key, value, sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key, value);

    commonTestUtils.produceSingleEvent(inputTopic, key, value);

    commonTestUtils.consumeEvent(Serdes.String().deserializer().getClass(),
        Serdes.String().deserializer().getClass(), outputTopic);

    List<List<SpanData>> traces = instrumentation.waitForTraces(3);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()));
  }




  @Test
  @DisplayName("Test KStream header capture with state store operation (GroupByKey -> Aggregate) using legacy KeyValueStore")
  void testKStreamStateStoreHeaderCaptureWithAggregateOpUsingLegacyStateStore() {
    String key = "key";
    String value = "val";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, String>as(
            Stores.persistentKeyValueStore("testStore"))
        .withKeySerde(Serdes.String()).withValueSerde(Serdes.String());
    streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String())).groupByKey()
        .aggregate(() -> "", (k, v, c) -> c + "," + v.charAt(0), materialized)
        .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    Properties properties = commonTestUtils.getPropertiesForStreams();
    kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic, outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key, value, sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key, value);

    commonTestUtils.produceSingleEvent(inputTopic, key, value);

    commonTestUtils.consumeEvent(Serdes.String().deserializer().getClass(),
        Serdes.String().deserializer().getClass(), outputTopic);

    List<List<SpanData>> traces = instrumentation.waitForTraces(3);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()));
  }

  @SneakyThrows
  @Test
  @DisplayName("Test KStream header capture with state store operation (GroupByKey -> Windowed -> Aggregate/Merge) using SessionStore")
  void testKStreamStateStoreHeaderCaptureWithAggregateOpUsingSessionStateStore() {
    String key = "key";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String())).groupByKey()
        .windowedBy(
            SessionWindows.with(Duration.ofMillis(5000)))
        .aggregate(() -> "", (k, v, c) -> c + "," + v, (k, v, c) -> c + ":" + v)
        .toStream((k, v) -> k.key() + " : " + k.window().toString())
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    Properties properties = commonTestUtils.getPropertiesForStreams();
    kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key, "1", sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key, "2");

    commonTestUtils.produceSingleEvent(inputTopic, key, "3");

    commonTestUtils.produceSingleEvent(inputTopic, key, "4");

    commonTestUtils.produceSingleEvent(inputTopic, key, "5");

    commonTestUtils.consumeAtLeastXEvents(Serdes.String().deserializer().getClass(),
        Serdes.String().deserializer().getClass(), outputTopic, 5);
    List<List<SpanData>> traces = instrumentation.waitForTraces(5);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStoreDelete(),
            produceChangelog(),
            produce(),
            consume(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStoreDelete(),
            produceChangelog(),
            produce(),
            consume(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStoreDelete(),
            produceChangelog(),
            produce(),
            consume(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStoreDelete(),
            produceChangelog(),
            produce(),
            consume(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume())
    );
  }

  @Test
  @DisplayName("Test KStream header capture with state store operation (GroupBy -> Count Aggregate) with caching enabled")
  void testKStreamStateStoreHeaderCaptureWithAggregateAndCaching() {
    String key = "key";
    String value = "test data";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    Properties properties = commonTestUtils.getPropertiesForStreams();
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> textLines = builder.stream(inputTopic,
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Long> wordCount = textLines
        .flatMapValues(val -> List.of(val.toLowerCase().split(" ")))
        .groupBy((k, val) -> val)
        .count(Materialized.as("WordCount"))
        .toStream();
    wordCount.print(Printed.toSysOut());
    wordCount.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    kafkaStreams = new KafkaStreams(builder.build(), properties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key+"1", value, sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key+"2", value);

    commonTestUtils.produceSingleEvent(inputTopic, key+"1", value);

    commonTestUtils.consumeEvent(Serdes.String().deserializer().getClass(),
        Serdes.Long().deserializer().getClass(), outputTopic);

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet().withLink(),
            stateStorePut(),
            produceChangelog(),
            produce(),
            consume()));
  }
}

/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURE_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CHARSET_UTF_8;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.NOT_WHITELISTED_HEADERS;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.cleanupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.setupHeaderConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.produceChangelog;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreCachePut;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreCacheRemove;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreFlush;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreGet;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStorePut;
import static io.opentelemetry.sdk.testing.assertj.TraceAssertData.trace;

import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
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
 * Tests for tracing propagation during Stream aggregation operations with caching enabled.
 */
public class KafkaStreamsStateStoreInstrumentationCacheTest {

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
  @DisplayName("Test KStream header capture with state store operation (FlatMap -> GroupBy -> Count Aggregate) with caching enabled")
  void testKStreamStateStoreHeaderCaptureWithAggregateAndCaching() {
    String key = "key";
    String value = "test data";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    Properties properties = commonTestUtils.getPropertiesForStreams();
    properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        "20000"); // re-enable state-store cache
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> textLines = builder.stream(inputTopic,
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Long> wordCount = textLines
        .flatMapValues(val -> List.of(val.toLowerCase().split(" ")))
        .groupBy((k, val) -> val)
        .count(Named.as("WordCount"))
        .toStream();
    wordCount.print(Printed.toSysOut());
    wordCount.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    kafkaStreams = new KafkaStreams(builder.build(), properties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value, sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key + "2", value);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value);

    commonTestUtils.consumeAtLeastXEvents(Serdes.String().deserializer().getClass(),
        Serdes.Long().deserializer().getClass(), outputTopic, 2);

    List<List<SpanData>> traces = instrumentation.waitForTraces(3);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS))
    );
  }

  @Test
  @DisplayName("Test KStream header capture with state store operation (FlatMap -> GroupBy -> Count Aggregate) with caching enabled using Legacy KV Store")
  void testKStreamStateStoreHeaderCaptureWithAggregateAndCachingUsingLegacyKVStore() {
    String key = "key";
    String value = "test data";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    Properties properties = commonTestUtils.getPropertiesForStreams();
    properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        "20000"); // re-enable state-store cache
    final StreamsBuilder builder = new StreamsBuilder();

    Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, Long>as(
            Stores.persistentKeyValueStore("testStore"))
        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long());

    final KStream<String, String> textLines = builder.stream(inputTopic,
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Long> wordCount = textLines
        .flatMapValues(val -> List.of(val.toLowerCase().split(" ")))
        .groupBy((k, val) -> val)
        .count(materialized)
        .toStream();
    wordCount.print(Printed.toSysOut());
    wordCount.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    kafkaStreams = new KafkaStreams(builder.build(), properties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value, sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key + "2", value);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value);

    commonTestUtils.consumeAtLeastXEvents(Serdes.String().deserializer().getClass(),
        Serdes.Long().deserializer().getClass(), outputTopic, 2);

    List<List<SpanData>> traces = instrumentation.waitForTraces(3);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS))
    );
  }

  @Test
  @DisplayName("Test KStream header capture with Session state store operation (FlatMap -> GroupBy -> WindowedBy SessionWindows->Count Aggregate) with caching enabled")
  void testKStreamSessionStateStoreHeaderCaptureWithAggregateAndCaching() {
    String key = "key";
    String value = "test data";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    Properties properties = commonTestUtils.getPropertiesForStreams();
    properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        "20000"); // re-enable state-store cache
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> textLines = builder.stream(inputTopic,
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Long> wordCount = textLines
        .flatMapValues(val -> List.of(val.toLowerCase().split(" ")))
        .groupBy((k, val) -> val)
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(5000))).count(
            Named.as("WordCount"))
        .toStream().map((k, v) -> new KeyValue<>(k.key() + " - " + k.window().toString(), v));
    wordCount.print(Printed.toSysOut());
    wordCount.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    kafkaStreams = new KafkaStreams(builder.build(), properties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value, sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key + "2", value);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value);

    commonTestUtils.consumeAtLeastXEvents(Serdes.String().deserializer().getClass(),
        Serdes.Long().deserializer().getClass(), outputTopic, 2);

    List<List<SpanData>> traces = instrumentation.waitForTraces(3);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCacheRemove().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCacheRemove().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCacheRemove().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCacheRemove().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS))
    );
  }

  @Test
  @DisplayName("Test KStream header capture with Windowed state store operation (FlatMap -> GroupBy -> WindowedBy Windows->Count Aggregate) with caching enabled")
  void testKStreamWindowedStateStoreHeaderCaptureWithAggregateAndCaching() {
    String key = "key";
    String value = "test data";

    Header[] sentHeaders = ArrayUtils.addAll(
        CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    Properties properties = commonTestUtils.getPropertiesForStreams();
    properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        "20000"); // re-enable state-store cache
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, String> textLines = builder.stream(inputTopic,
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, Long> wordCount = textLines
        .flatMapValues(val -> List.of(val.toLowerCase().split(" ")))
        .groupBy((k, val) -> val)
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(5000))).count(
            Named.as("WordCount"))
        .toStream().map((k, v) -> new KeyValue<>(k.key() + " - " + k.window().toString(), v));
    wordCount.print(Printed.toSysOut());
    wordCount.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    kafkaStreams = new KafkaStreams(builder.build(), properties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value, sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key + "2", value);

    commonTestUtils.produceSingleEvent(inputTopic, key + "1", value);

    commonTestUtils.consumeAtLeastXEvents(Serdes.String().deserializer().getClass(),
        Serdes.Long().deserializer().getClass(), outputTopic, 2);

    List<List<SpanData>> traces = instrumentation.waitForTraces(3);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume()
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produce().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withNameContaining("-repartition")
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreGet().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreCachePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStoreFlush().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog(),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)
                .withoutHeaders(NOT_WHITELISTED_HEADERS))
    );
  }
}

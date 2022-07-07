/*
 * Copyright 2022 Confluent Inc.
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
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStoreGet;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData.stateStorePut;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.TraceAssertData.trace;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests verifying State Store restoration for tracing propagation during Stream aggregation
 * operations that produce KTable and utilize StateStore put/get . Using groupByKey().count() and
 * groupByKey().aggregate().
 */
public class KafkaStreamsStateStoreInstrumentationRestoreTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String inputTopic;
  private String outputTopic;

  private CommonTestUtils commonTestUtils;

  CountDownLatch streamsLatch = new CountDownLatch(1);

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
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
    inputTopic = "input-topic-" + UUID.randomUUID();
    outputTopic = "output-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    streamsLatch.countDown();
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }

  static Stream<Arguments> topologyCombinations() {
    return Stream.of(Arguments.of(
            "Default state store",
            (TriConsumer<StreamsBuilder, String, String>) (streamsBuilder, inputTopic, outputTopic) -> streamsBuilder.stream(
                    inputTopic,
                    Consumed.with(Serdes.String(), Serdes.String())).groupByKey()
                .aggregate(() -> "", (k, v, c) -> c + "," + v, Materialized.as("test-state-store"))
                .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))),
        Arguments.of(
            "Legacy state store",
            (TriConsumer<StreamsBuilder, String, String>) (streamsBuilder, inputTopic, outputTopic) -> {
              Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, String>as(
                      Stores.persistentKeyValueStore("testStore"))
                  .withKeySerde(Serdes.String()).withValueSerde(Serdes.String());
              streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                  .groupByKey()
                  .aggregate(() -> "", (k, v, c) -> c + "," + v, materialized)
                  .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
            }
        ));
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("topologyCombinations")
  @DisplayName("Test KStream tracing with state store restoration using (GroupByKey -> Aggregate)")
  void testKStreamStateStoreRestoration(String testName,
      TriConsumer<StreamsBuilder, String, String> streamsBuilderTopologyProvider) {
    Properties consumerPropertyOverrides = new Properties();
    consumerPropertyOverrides.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,
        "test-consumer-" + UUID.randomUUID());
    consumerPropertyOverrides.put(ConsumerConfig.GROUP_ID_CONFIG,
        "test-consumer-group-" + UUID.randomUUID());

    Properties streamsProperties = commonTestUtils.getPropertiesForStreams();

    Header[] sentHeaders = ArrayUtils.addAll(CAPTURE_WHITELISTED_HEADERS, NOT_WHITELISTED_HEADERS);

    String key = "key";

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilderTopologyProvider.accept(streamsBuilder, inputTopic, outputTopic);
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);
    commonTestUtils.createTopologyAndStartKStream(kafkaStreams, streamsLatch, inputTopic,
        outputTopic);

    commonTestUtils.produceSingleEvent(inputTopic, key, "v1", sentHeaders);

    commonTestUtils.produceSingleEvent(inputTopic, key, "v2");

    commonTestUtils.produceSingleEvent(inputTopic, key, "v3");

    commonTestUtils.consumeAtLeastXEvents(Serdes.String().deserializer().getClass(),
        Serdes.String().deserializer().getClass(), outputTopic, 3, consumerPropertyOverrides);

    instrumentation.waitForTraces(3);

    kafkaStreams.close();
    kafkaStreams.cleanUp();
    kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);
    kafkaStreams.start();
    commonTestUtils.produceSingleEvent(inputTopic, key, "v4");

    commonTestUtils.consumeAtLeastXEvents(Serdes.String().deserializer().getClass(),
        Serdes.String().deserializer().getClass(), outputTopic, 1, consumerPropertyOverrides);

    List<List<SpanData>> traces = instrumentation.waitForTraces(4);

    //Make sure all spans recorded for last trace before asserting.
    await().atMost(Duration.ofSeconds(1))
        .until(() -> traces.size() == 4 && traces.get(3).size() == 7);

    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            produceChangelog().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            produce().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURE_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            consume().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            stateStoreGet().withLink().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            produceChangelog().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)),
        trace().withSpans(
            produce().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            consume().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            stateStoreGet().withLink().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            produceChangelog().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)),
        trace().withSpans(
            produce().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            consume().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            stateStoreGet().withLink().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            stateStorePut().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            produceChangelog().withoutHeaders(CAPTURE_WHITELISTED_HEADERS),
            produce().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, CAPTURED_PROPAGATED_HEADERS)));
  }
}

/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ObjectMapperUtil;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
    inputTopic = "input-topic-" + UUID.randomUUID();
    outputTopic = "output-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }

  public static Stream<Arguments> simpleKeysAndValuesTestParameters() {
    return Stream.of(
        Arguments.of(Serdes.Short(), (short) 10, (short) 20),
        Arguments.of(Serdes.Long(), 100L, 1000L),
        Arguments.of(Serdes.Integer(), 110, 1100),
        Arguments.of(Serdes.UUID(), UUID.randomUUID(), UUID.randomUUID()),
        Arguments.of(Serdes.Float(), 1.5f, 15.2f),
        Arguments.of(Serdes.Double(), 12.345, 123.456),
        Arguments.of(Serdes.String(), "keyString", "valueString"));
  }

  @ParameterizedTest
  @MethodSource("simpleKeysAndValuesTestParameters")
  @DisplayName("Test KStream send to changelog span captures payload for simple key / value types")
  void testKStreamSendToChanglogSpanCapturesPayloadForSimpleKeysAndValues(Serde serde, Object key,
      Object value) {
    testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(serde, serde, key, value,
        key.toString());
  }

  @Test
  @DisplayName("Test KStream send to changelog span captures payload for ByteArray key / value as Base64 encoded String")
  void testKStreamSendToChanglogSpanCapturesPayloadForByteArrayKeysAndValuesAsBase64String() {
    byte[] key = new byte[]{23, 42, 31, 95};
    byte[] value = new byte[]{54, 23, 65, 93, 69};
    String expectedKey = Base64.getEncoder().encodeToString(key);
    Serde serde = Serdes.ByteArray();
    testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(serde, serde, key, value,
        expectedKey);
  }

  @Test
  @DisplayName("Test KStream send to changelog span captures payload for ByteBuffer key / value as Base64 encoded String")
  void testKStreamSendToChanglogSpanCapturesPayloadForByteBufferKeysAndValuesAsBase64String() {
    ByteBuffer key = ByteBuffer.wrap(new byte[]{23, 42, 31, 95});
    ByteBuffer value = ByteBuffer.wrap(new byte[]{54, 23, 65, 93, 69});
    String expectedKey = Base64.getEncoder().encodeToString(key.array());
    Serde serde = Serdes.ByteBuffer();
    testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(serde, serde, key, value,
        expectedKey);
  }


  @Test
  @DisplayName("Test KStream send to changelog span captures payload for Json String key / value as flattened attributes")
  void testKStreamSendToChanglogSpanCapturesPayloadForJsonSerializedAsStringKeysAndValuesAsFlattenedSetOfAttributes() {
    String key = "{\n"
        + "\"key1\":\"key1Value\",\n"
        + "\"key2\":\"key2Value\"\n"
        + "}";
    String value = "{\"val1\":\"value1\", \"val2\":\"value2\"}";

    Serde serde = Serdes.String();

    createTopologyAndInjectMessagesToGenerateTrace(serde, serde, key, value);

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);
    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(key);
          assertThat(attributes.get(AttributeKey.stringKey("payload.key"))).isNull();
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key1")))
              .isEqualTo("key1Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key2")))
              .isEqualTo("key2Value");
        };
    assertChangeLogSendTraceAndPayloadAttributes(traces, attributeAssertions);
  }

  @Test
  @DisplayName("Test KStream send to changelog span captures payload for Json Pojo key / value as flattened attributes")
  void testKStreamSendToChanglogSpanCapturesPayloadForJsonSerializedAsPojoKeysAndValuesAsFlattenedSetOfAttributes() {
    TestKeyPojo keyPojo = new TestKeyPojo("key1Value", "key2Value");
    TestValuePojo valuePojo = new TestValuePojo("value1", "value2");

    ObjectMapper objectMapper = new ObjectMapper();
    ObjectMapperUtil objectMapperUtil = new ObjectMapperUtil(objectMapper);
    String expectedKey = objectMapperUtil.mapObjectToJSon(keyPojo);

    Serde keySerde = new JsonTestKeyPojoSerde();
    Serde valueSerde = new JsonTestValuePojoSerde();
    createTopologyAndInjectMessagesToGenerateTrace(keySerde, valueSerde, keyPojo, valuePojo);

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);

    Consumer<Attributes> attributeAssertions =
        (attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(expectedKey);
          assertThat(attributes.get(AttributeKey.stringKey("payload.key"))).isNull();
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key1")))
              .isEqualTo("key1Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key2")))
              .isEqualTo("key2Value");
        });
    assertChangeLogSendTraceAndPayloadAttributes(traces, attributeAssertions);
  }

  private void testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(Serde keySerde,
      Serde valueSerde, Object key, Object value,
      String expectedKey) {

    createTopologyAndInjectMessagesToGenerateTrace(keySerde, valueSerde, key, value);
    waitForTraceCaptureAndAssertSpanAttributes(expectedKey);
  }


  private void waitForTraceCaptureAndAssertSpanAttributes(String expectedKey) {
    List<List<SpanData>> traces = instrumentation.waitForTraces(3);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(expectedKey);

          assertThat(attributes.get(AttributeKey.stringKey("payload.key")))
              .isEqualTo(expectedKey);
        };

    assertChangeLogSendTraceAndPayloadAttributes(traces, attributeAssertions);
  }

  private Consumer<Attributes> valueAttributeAssertions(int expectedValue) {
    return attributes -> {
      assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
          .contains("{\"value\":" + expectedValue + ",\"timestamp\":");

      assertThat(attributes.get(AttributeKey.stringKey("payload.value.value")))
          .isEqualTo(String.valueOf(expectedValue));
      assertThat(attributes.get(AttributeKey.stringKey("payload.value.timestamp")))
          .isNotBlank();
    };
  }

  private void assertChangeLogSendTraceAndPayloadAttributes(List<List<SpanData>> traces,
      Consumer<Attributes> attributeAssertions) {

    TracesAssert.assertThat(traces).hasSize(3)
        .hasTracesSatisfyingExactly(
            trace -> trace
                .hasSize(6)
                .hasSpansSatisfyingExactly(
                    span -> span.hasKind(SpanKind.PRODUCER), //Input produce
                    span -> span.hasKind(SpanKind.CONSUMER), //Input consume
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(1))
                    ), //StateStore PUT
                    span -> span.hasKind(SpanKind.PRODUCER), //ChangeLog produce
                    span -> span.hasKind(SpanKind.PRODUCER), //Output produce

                    span -> span.hasKind(SpanKind.CONSUMER) //Output consume
                ),
            trace -> trace
                .hasSize(7)
                .hasSpansSatisfyingExactly(
                    span -> span.hasKind(SpanKind.PRODUCER), //Input produce
                    span -> span.hasKind(SpanKind.CONSUMER), //Input consume
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(1))
                    ), //StateStore GET
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(2))
                    ), //StateStore PUT
                    span -> span.hasKind(SpanKind.PRODUCER), //ChangeLog produce
                    span -> span.hasKind(SpanKind.PRODUCER), //Output produce

                    span -> span.hasKind(SpanKind.CONSUMER) //Output consume
                ),
            trace -> trace
                .hasSize(7)
                .hasSpansSatisfyingExactly(
                    span -> span.hasKind(SpanKind.PRODUCER), //Input produce
                    span -> span.hasKind(SpanKind.CONSUMER), //Input consume

                    //StateStore GET
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(2))
                    ),

                    //StateStore PUT
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(3))
                    ),

                    //ChangeLog produce
                    span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(3))
                    ),

                    //Output produce
                    span -> span.hasKind(SpanKind.PRODUCER)
                        .hasAttributesSatisfying(attributeAssertions.andThen(
                            attributes -> assertThat(
                                attributes.get(AttributeKey.stringKey("payload.value")))
                                .isEqualTo("3"))),

                    span -> span.hasKind(SpanKind.CONSUMER) //Output consume
                ));
  }

  private void createTopologyAndInjectMessagesToGenerateTrace(Serde keySerde, Serde valueSerde,
      Object key, Object value) {
    KafkaStreams kafkaStreams = prepareKStreamTopology(keySerde, valueSerde);
    createTopologyAndInjectMessagesToGenerateTrace(keySerde, valueSerde, key, value, kafkaStreams);
  }

  private void createTopologyAndInjectMessagesToGenerateTrace(Serde keySerde, Serde valueSerde,
      Object key, Object value, KafkaStreams kafkaStreams) {
    AdminClient adminClient = KafkaAdminClient.create(
        commonTestUtils.getKafkaProperties(new Properties()));
    adminClient.createTopics(Arrays.asList(
        new NewTopic(inputTopic, 1, (short) 1),
        new NewTopic(outputTopic, 1, (short) 1)));

    CountDownLatch streamsLatch = new CountDownLatch(1);
    new Thread(() -> {
      kafkaStreams.start();
      try {
        streamsLatch.await();
        kafkaStreams.close();
      } catch (InterruptedException e) {
        kafkaStreams.close();
        Thread.currentThread().interrupt();
      }
    }).start();

    commonTestUtils.produceSingleEvent(keySerde.serializer().getClass(),
        valueSerde.serializer().getClass(), inputTopic, key, value);

    commonTestUtils.produceSingleEvent(keySerde.serializer().getClass(),
        valueSerde.serializer().getClass(), inputTopic, key, value);

    commonTestUtils.produceSingleEvent(keySerde.serializer().getClass(),
        valueSerde.serializer().getClass(), inputTopic, key, value);

    commonTestUtils.consumeEvent(keySerde.deserializer().getClass(),
        Serdes.Long().deserializer().getClass(), outputTopic);
    streamsLatch.countDown();
  }

  private KafkaStreams prepareKStreamTopology(Serde keySerde, Serde valueSerde) {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream(inputTopic, Consumed.with(keySerde, valueSerde)).groupByKey().count()
        .toStream().to(outputTopic, Produced.with(keySerde, Serdes.Long()));
    return new KafkaStreams(streamsBuilder.build(), commonTestUtils.getPropertiesForStreams());
  }

  @Test
  @DisplayName("Test KStream send to changelog span captures payload with custom aggregate")
  void testKStreamSendToChanglogSpanCapturesPayloadForShortWithCustomAggregate() {
    Short key = 1;
    Short value = 5;
    KafkaStreams kafkaStreams = prepareKStreamTopologyWithCustomAggregation();
    createTopologyAndInjectMessagesToGenerateTrace(Serdes.Short(), Serdes.Short(), key, value,
        kafkaStreams);
    List<List<SpanData>> traces = instrumentation.waitForTraces(3);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(key.toString());

          assertThat(attributes.get(AttributeKey.stringKey("payload.key")))
              .isEqualTo(key.toString());
        };

    TracesAssert.assertThat(traces).hasSize(3)
        .hasTracesSatisfyingExactly(
            trace -> trace
                .hasSize(6)
                .hasSpansSatisfyingExactly(
                    span -> span.hasKind(SpanKind.PRODUCER), //Input produce
                    span -> span.hasKind(SpanKind.CONSUMER), //Input consume
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(2))
                    ), //StateStore PUT
                    span -> span.hasKind(SpanKind.PRODUCER), //ChangeLog produce
                    span -> span.hasKind(SpanKind.PRODUCER), //Output produce

                    span -> span.hasKind(SpanKind.CONSUMER) //Output consume
                ),
            trace -> trace
                .hasSize(7)
                .hasSpansSatisfyingExactly(
                    span -> span.hasKind(SpanKind.PRODUCER), //Input produce
                    span -> span.hasKind(SpanKind.CONSUMER), //Input consume
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(2))
                    ), //StateStore GET
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(6))
                    ), //StateStore PUT
                    span -> span.hasKind(SpanKind.PRODUCER), //ChangeLog produce
                    span -> span.hasKind(SpanKind.PRODUCER), //Output produce

                    span -> span.hasKind(SpanKind.CONSUMER) //Output consume
                ),
            trace -> trace
                .hasSize(7)
                .hasSpansSatisfyingExactly(
                    span -> span.hasKind(SpanKind.PRODUCER), //Input produce
                    span -> span.hasKind(SpanKind.CONSUMER), //Input consume

                    //StateStore GET
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(6))
                    ),

                    //StateStore PUT
                    span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(14))
                    ),

                    //ChangeLog produce
                    span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                        attributeAssertions.andThen(valueAttributeAssertions(14))
                    ),

                    //Output produce
                    span -> span.hasKind(SpanKind.PRODUCER)
                        .hasAttributesSatisfying(attributeAssertions.andThen(
                            attributes -> assertThat(
                                attributes.get(AttributeKey.stringKey("payload.value")))
                                .isEqualTo("14"))),

                    span -> span.hasKind(SpanKind.CONSUMER) //Output consume
                ));
  }

  private KafkaStreams prepareKStreamTopologyWithCustomAggregation() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream(inputTopic, Consumed.with(Serdes.Short(), Serdes.Short())).groupByKey()
        .aggregate(() -> 0L, (k, v, c) -> (c + 1) * 2)
        .toStream().to(outputTopic, Produced.with(Serdes.Short(), Serdes.Long()));
    Properties properties = commonTestUtils.getPropertiesForStreams();
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.Long().getClass().getName());
    return new KafkaStreams(streamsBuilder.build(), properties);
  }
}

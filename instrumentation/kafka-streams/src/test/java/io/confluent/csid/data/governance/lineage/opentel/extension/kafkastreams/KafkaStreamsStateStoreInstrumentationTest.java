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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
 * Tests for tracing propagation during Stream to KTable join operations that utilize StateStore
 * put/get.
 */
public class KafkaStreamsStateStoreInstrumentationTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String inputTopic;
  private String ktableTopic;
  private String outputTopic;

  private CommonTestUtils commonTestUtils;

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
    inputTopic = "input-topic-" + UUID.randomUUID();
    ktableTopic = "ktable-topic-" + UUID.randomUUID();
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
    testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(serde, key, value, key.toString(),
        value.toString());
  }

  @Test
  @DisplayName("Test KStream send to changelog span captures payload for ByteArray key / value as Base64 encoded String")
  void testKStreamSendToChanglogSpanCapturesPayloadForByteArrayKeysAndValuesAsBase64String() {
    byte[] key = new byte[]{23, 42, 31, 95};
    byte[] value = new byte[]{54, 23, 65, 93, 69};
    String expectedKey = Base64.getEncoder().encodeToString(key);
    String expectedValue = Base64.getEncoder().encodeToString(value);
    Serde serde = Serdes.ByteArray();
    testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(serde, key, value, expectedKey,
        expectedValue);
  }

  @Test
  @DisplayName("Test KStream send to changelog span captures payload for ByteBuffer key / value as Base64 encoded String")
  void testKStreamSendToChanglogSpanCapturesPayloadForByteBufferKeysAndValuesAsBase64String() {
    ByteBuffer key = ByteBuffer.wrap(new byte[]{23, 42, 31, 95});
    ByteBuffer value = ByteBuffer.wrap(new byte[]{54, 23, 65, 93, 69});
    String expectedKey = Base64.getEncoder().encodeToString(key.array());
    String expectedValue = Base64.getEncoder().encodeToString(value.array());
    Serde serde = Serdes.ByteBuffer();
    testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(serde, key, value, expectedKey,
        expectedValue);
  }


  @Test
  @DisplayName("Test KStream send to changelog span captures payload for Json String key / value as flattened attributes")
  void testKStreamSendToChanglogSpanCapturesPayloadForJsonSerializedAsStringKeysAndValuesAsFlattenedSetOfAttributes() {
    String key = "{\n"
        + "\"key1\":\"key1Value\",\n"
        + "\"key2\":\"key2Value\"\n"
        + "}";
    String value = "{\"val1\":\"value1\", \"val2\":\"value2\"}";
    String expectedValue = "{\\\"val1\\\":\\\"value1\\\", \\\"val2\\\":\\\"value2\\\"}";

    Serde serde = Serdes.String();

    createTopologyAndInjectMessagesToGenerateTrace(serde, serde, key, value);

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);
    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(key);
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
              .contains("{\"value\":\"" + expectedValue + "\"");
          assertThat(attributes.get(AttributeKey.stringKey("payload.key"))).isNull();
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key1")))
              .isEqualTo("key1Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key2")))
              .isEqualTo("key2Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.value"))).isNull();
          assertThat(attributes.get(AttributeKey.stringKey("payload.value.value")))
              .isEqualTo(value);
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
    String expectedValue = objectMapperUtil.mapObjectToJSon(valuePojo);

    Serde keySerde = new JsonTestKeyPojoSerde();
    Serde valueSerde = new JsonTestValuePojoSerde();
    createTopologyAndInjectMessagesToGenerateTrace(keySerde, valueSerde, keyPojo, valuePojo);

    List<List<SpanData>> traces = instrumentation.waitForTraces(2);

    Consumer<Attributes> attributeAssertions =
        (attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(expectedKey);
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
              .contains("{\"value\":" + expectedValue);
          assertThat(attributes.get(AttributeKey.stringKey("payload.key"))).isNull();
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key1")))
              .isEqualTo("key1Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key2")))
              .isEqualTo("key2Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.value"))).isNull();
          assertThat(
              attributes.get(AttributeKey.stringKey("payload.value.value.value1")))
              .isEqualTo("value1");
          assertThat(
              attributes.get(AttributeKey.stringKey("payload.value.value.value2")))
              .isEqualTo("value2");
        });
    assertChangeLogSendTraceAndPayloadAttributes(traces, attributeAssertions);
  }

  private void testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(Serde keySerde,
      Serde valueSerde, Object key, Object value,
      String expectedKey, String expectedValue) {

    createTopologyAndInjectMessagesToGenerateTrace(keySerde, valueSerde, key, value);
    waitForTraceCaptureAndAssertSpanAttributes(expectedKey, expectedValue, value.getClass());
  }

  private void testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(Serde serde, Object key,
      Object value,
      String expectedKey, String expectedValue) {
    testKStreamSendToChangelogSpanCapturesPayloadKeysAndValue(serde, serde, key, value, expectedKey,
        expectedValue);
  }

  private void waitForTraceCaptureAndAssertSpanAttributes(String expectedKey, String expectedValue,
      Class<?> valueClass) {
    List<List<SpanData>> traces = instrumentation.waitForTraces(2);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(expectedKey);
          // As the value is embedded in composite ValueAndTimestamp object
          // all attributes that are captured as string value need to have surrounding quotes - i.e. "field":"value" as opposed
          // to other types - i.e. "integer_field":5
          if (valueClass.equals(String.class) || valueClass.equals(UUID.class)
              || valueClass.equals(byte[].class) || valueClass.getSimpleName()
              .equals("HeapByteBuffer")) {
            assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
                .contains("{\"value\":\"" + expectedValue + "\"");
          } else {
            assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
                .contains("{\"value\":" + expectedValue);
          }

          assertThat(attributes.get(AttributeKey.stringKey("payload.key")))
              .isEqualTo(expectedKey);
          assertThat(attributes.get(AttributeKey.stringKey("payload.value.value")))
              .isEqualTo(expectedValue);
          assertThat(attributes.get(AttributeKey.stringKey("payload.value.timestamp")))
              .isNotBlank();

        };

    assertChangeLogSendTraceAndPayloadAttributes(traces, attributeAssertions);
  }

  private void assertChangeLogSendTraceAndPayloadAttributes(List<List<SpanData>> traces,
      Consumer<Attributes> attributeAssertions) {

    TracesAssert.assertThat(traces).hasSize(2)
        .hasTracesSatisfyingExactly(
            trace -> trace
                .hasSize(4)
                .hasSpansSatisfyingExactly(
                    span -> span
                        .hasKind(SpanKind.PRODUCER),
                    span -> span.hasKind(SpanKind.CONSUMER),
                    span -> span.hasKind(SpanKind.INTERNAL) // StateStore PUT
                        .hasAttributesSatisfying(attributeAssertions),
                    span -> span
                        .hasKind(SpanKind.PRODUCER) // Change log send
                        .hasAttributesSatisfying(attributeAssertions)
                ),
            trace -> trace
                .hasSize(5)
                .hasSpansSatisfyingExactly(
                    span -> span.hasKind(SpanKind.PRODUCER),
                    span -> span.hasKind(SpanKind.CONSUMER),
                    span -> span.hasKind(SpanKind.INTERNAL) // StateStore GET
                        .hasAttributesSatisfying(attributeAssertions).hasTotalRecordedLinks(1),
                    span -> span.hasKind(SpanKind.PRODUCER),
                    span -> span.hasKind(SpanKind.CONSUMER)
                ));
  }

  private void createTopologyAndInjectMessagesToGenerateTrace(Serde keySerde, Serde valueSerde,
      Object key, Object value) {
    KafkaStreams kafkaStreams = prepareKStreamTopologyWithKTable(keySerde, valueSerde);
    AdminClient adminClient = KafkaAdminClient.create(
        commonTestUtils.getKafkaProperties(new Properties()));
    adminClient.createTopics(Arrays.asList(
        new NewTopic(inputTopic, 1, (short) 1),
        new NewTopic(ktableTopic, 1, (short) 1),
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
        valueSerde.serializer().getClass(), ktableTopic, key, value);
    commonTestUtils.produceSingleEvent(keySerde.serializer().getClass(),
        valueSerde.serializer().getClass(), inputTopic, key, value);

    commonTestUtils.consumeEvent(keySerde.deserializer().getClass(),
        valueSerde.deserializer().getClass(), outputTopic);
    streamsLatch.countDown();
  }

  private KafkaStreams prepareKStreamTopologyWithKTable(Serde keySerde, Serde valueSerde) {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KTable table = streamsBuilder.table(ktableTopic, Consumed.with(keySerde, valueSerde),
        Materialized.with(keySerde, valueSerde));
    streamsBuilder.stream(inputTopic, Consumed.with(keySerde, valueSerde)).mapValues(x -> x)
        .join(table, (value1, value2) -> value2)
        .to(outputTopic, Produced.with(keySerde, valueSerde));
    return new KafkaStreams(streamsBuilder.build(), commonTestUtils.getPropertiesForStreams());
  }
}

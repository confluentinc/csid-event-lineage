/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ExtendedKafkaProducerInstrumentationTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;
  private CommonTestUtils commonTestUtils;

  @BeforeEach
  void setup() {
    testTopic = "test-topic-" + UUID.randomUUID();
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
  }

  @AfterEach
  void teardown() {
    commonTestUtils.stopKafkaContainer();
  }

  public static Stream<Arguments> simpleKeysAndValuesTestParameters() {
    return Stream.of(
        Arguments.of(ShortSerializer.class, (short) 10, (short) 20),
        Arguments.of(LongSerializer.class, 100L, 1000L),
        Arguments.of(IntegerSerializer.class, 110, 1100),
        Arguments.of(UUIDSerializer.class, UUID.randomUUID(), UUID.randomUUID()),
        Arguments.of(FloatSerializer.class, 1.5f, 15.2f),
        Arguments.of(DoubleSerializer.class, 12.345, 123.456),
        Arguments.of(StringSerializer.class, "keyString", "valueString"));
  }

  @ParameterizedTest
  @MethodSource("simpleKeysAndValuesTestParameters")
  @DisplayName("Test producer send span captures payload for simple key / value types")
  void testProducerSpanCapturesPayloadForSimpleKeysAndValues(Class<?> serializerClass, Object key,
      Object value) {
    testProducerSpanCapturesPayloadForKeyValuePair(serializerClass, key, value, key.toString(),
        value.toString());
  }

  @Test
  @DisplayName("Test producer send span captures payload for ByteArray key / value as Base64 string")
  void testProducerSpanCapturesPayloadForByteArrayKeysAndValuesAsBase64String() {
    byte[] key = new byte[]{23, 42, 31, 95};
    byte[] value = new byte[]{54, 23, 65, 93, 69};
    String expectedKey = Base64.getEncoder().encodeToString(key);
    String expectedValue = Base64.getEncoder().encodeToString(value);
    Class<?> serializerClass = ByteArraySerializer.class;
    testProducerSpanCapturesPayloadForKeyValuePair(serializerClass, key, value, expectedKey,
        expectedValue);
  }

  @Test
  @DisplayName("Test producer send span captures payload for ByteBuffer key / value as Base64 string")
  void testProducerSpanCapturesPayloadForByteBufferKeysAndValuesAsBase64String() {
    ByteBuffer key = ByteBuffer.wrap(new byte[]{23, 42, 31, 95});
    ByteBuffer value = ByteBuffer.wrap(new byte[]{54, 23, 65, 93, 69});
    String expectedKey = Base64.getEncoder().encodeToString(key.array());
    String expectedValue = Base64.getEncoder().encodeToString(value.array());
    Class<?> serializerClass = ByteBufferSerializer.class;
    testProducerSpanCapturesPayloadForKeyValuePair(serializerClass, key, value, expectedKey,
        expectedValue);
  }

  private void testProducerSpanCapturesPayloadForKeyValuePair(Class<?> serializerClass, Object key,
      Object value,
      String expectedKey, String expectedValue) {
    commonTestUtils.produceSingleEvent(serializerClass, serializerClass, testTopic, key, value);

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(expectedKey);
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
              .isEqualTo(expectedValue);
          assertThat(attributes.get(AttributeKey.stringKey("payload.key")))
              .isEqualTo(expectedKey);
          assertThat(attributes.get(AttributeKey.stringKey("payload.value")))
              .isEqualTo(expectedValue);
        };

    assertProduceTraceAndPayloadAttributes(traces, attributeAssertions);
  }

  @Test
  @DisplayName("Test producer send span captures payload for Json String key / value as flattened attributes")
  void testProducerSpanCapturesPayloadForJsonSerializedAsStringKeysAndValuesAsFlattenedSetOfAttributes() {
    String key = "{\n"
        + "\"key1\":\"key1Value\",\n"
        + "\"key2\":\"key2Value\"\n"
        + "}";
    String value = "{\n"
        + "\"val1\":\"value1\",\n"
        + "\"val2\":\"value2\"\n"
        + "}";
    Class<?> serializerClass = StringSerializer.class;

    commonTestUtils.produceSingleEvent(serializerClass, serializerClass, testTopic, key, value);

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(key);
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
              .isEqualTo(value);
          assertThat(attributes.get(AttributeKey.stringKey("payload.key"))).isNull();
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key1")))
              .isEqualTo("key1Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.key.key2")))
              .isEqualTo("key2Value");
          assertThat(attributes.get(AttributeKey.stringKey("payload.value"))).isNull();
          assertThat(attributes.get(AttributeKey.stringKey("payload.value.val1")))
              .isEqualTo("value1");
          assertThat(attributes.get(AttributeKey.stringKey("payload.value.val2")))
              .isEqualTo("value2");
        };
    assertProduceTraceAndPayloadAttributes(traces, attributeAssertions);
  }

  private void assertProduceTraceAndPayloadAttributes(List<List<SpanData>> traces,
      Consumer<Attributes> attributeAssertions) {

    TracesAssert.assertThat(traces).hasSize(1)
        .hasTracesSatisfyingExactly(
            trace -> trace
                .hasSize(1)
                .hasSpansSatisfyingExactly(span ->
                    span.hasKind(SpanKind.PRODUCER)
                        .hasAttributesSatisfying(attributeAssertions)));
  }
}

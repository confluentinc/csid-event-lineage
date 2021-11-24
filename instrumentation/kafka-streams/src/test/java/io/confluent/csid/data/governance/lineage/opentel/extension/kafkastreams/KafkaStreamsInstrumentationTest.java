/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ObjectMapperUtil;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class KafkaStreamsInstrumentationTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String inputTopic;
  private String outputTopic;
  private CommonTestUtils commonTestUtils;

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    inputTopic = "input-topic-" + UUID.randomUUID();
    outputTopic = "output-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
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
  @DisplayName("Test KStream process span captures payload for simple key / value types")
  void testKStreamProcessSpanCapturesPayloadForSimpleKeysAndValues(Serde serde, Object key,
      Object value) {
    testKStreamProcessSpanCapturesPayloadKeysAndValue(serde, serde, key, value, key.toString(),
        value.toString());
  }

  @Test
  @DisplayName("Test KStream process span captures payload for ByteArray key / value as Base64 encoded String")
  void testKStreamProcessSpanCapturesPayloadForByteArrayKeysAndValuesAsBase64String() {
    byte[] key = new byte[]{23, 42, 31, 95};
    byte[] value = new byte[]{54, 23, 65, 93, 69};
    String expectedKey = Base64.getEncoder().encodeToString(key);
    String expectedValue = Base64.getEncoder().encodeToString(value);
    Serde serde = Serdes.ByteArray();
    testKStreamProcessSpanCapturesPayloadKeysAndValue(serde, serde, key, value, expectedKey,
        expectedValue);
  }

  @Test
  @DisplayName("Test KStream process span captures payload for ByteBuffer key / value as Base64 encoded String")
  void testKStreamProcessSpanCapturesPayloadForByteBufferKeysAndValuesAsBase64String() {
    ByteBuffer key = ByteBuffer.wrap(new byte[]{23, 42, 31, 95});
    ByteBuffer value = ByteBuffer.wrap(new byte[]{54, 23, 65, 93, 69});
    String expectedKey = Base64.getEncoder().encodeToString(key.array());
    String expectedValue = Base64.getEncoder().encodeToString(value.array());
    Serde serde = Serdes.ByteBuffer();
    testKStreamProcessSpanCapturesPayloadKeysAndValue(serde, serde, key, value, expectedKey,
        expectedValue);
  }


  @Test
  @DisplayName("Test KStream process span captures payload for Json String key / value as flattened attributes")
  void testKStreamProcessSpanCapturesPayloadForJsonSerializedAsStringKeysAndValuesAsFlattenedSetOfAttributes() {
    String key = "{\n"
        + "\"key1\":\"key1Value\",\n"
        + "\"key2\":\"key2Value\"\n"
        + "}";
    String value = "{\n"
        + "\"val1\":\"value1\",\n"
        + "\"val2\":\"value2\"\n"
        + "}";
    Serde serde = Serdes.String();

    TopologyTestDriver topologyTestDriver = prepareKStreamTopology(serde, serde);
    TestInputTopic testInputTopic = createTestInputTopic(topologyTestDriver, key, value, serde,
        serde);
    TestOutputTopic testOutputTopic = createTestOutputTopic(topologyTestDriver, serde, serde);
    testInputTopic.pipeInput(key, value);

    await().atMost(Duration.ofSeconds(5)).until(() -> !testOutputTopic.isEmpty());

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(key);
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
              .contains(value);
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.key"))).isNull();
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.key.key1")))
              .isEqualTo("key1Value");
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.key.key2")))
              .isEqualTo("key2Value");
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.value"))).isNull();
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.value.val1")))
              .isEqualTo("value1");
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.value.val2")))
              .isEqualTo("value2");
        };
    assertProcessTraceAndPayloadAttributes(traces, attributeAssertions);

    KeyValue outputMessage = testOutputTopic.readKeyValue();
    Assertions.assertThat(outputMessage.key).isEqualTo(key);
    Assertions.assertThat(outputMessage.value).isEqualTo(value);
  }

  @Test
  @DisplayName("Test KStream process span captures payload for Json Pojo key / value as flattened attributes")
  void testKStreamProcessSpanCapturesPayloadForJsonSerializedAsPojoKeysAndValuesAsFlattenedSetOfAttributes() {
    TestKeyPojo keyPojo = new TestKeyPojo("key1Value", "key2Value");
    TestValuePojo valuePojo = new TestValuePojo("value1", "value2");

    ObjectMapper objectMapper = new ObjectMapper();
    String expectedKey = ObjectMapperUtil.mapObjectToJSon(keyPojo, objectMapper);
    String expectedValue = ObjectMapperUtil.mapObjectToJSon(valuePojo, objectMapper);

    Serde keySerde = new JsonTestKeyPojoSerde();
    Serde valueSerde = new JsonTestValuePojoSerde();

    TopologyTestDriver topologyTestDriver = prepareKStreamTopology(keySerde, valueSerde);
    TestInputTopic testInputTopic = createTestInputTopic(topologyTestDriver, keyPojo, valuePojo,
        keySerde, valueSerde);
    TestOutputTopic testOutputTopic = createTestOutputTopic(topologyTestDriver, keySerde,
        valueSerde);
    testInputTopic.pipeInput(keyPojo, valuePojo);

    await().atMost(Duration.ofSeconds(5)).until(() -> !testOutputTopic.isEmpty());

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(expectedKey);
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
              .contains(expectedValue);
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.key"))).isNull();
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.key.key1")))
              .isEqualTo("key1Value");
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.key.key2")))
              .isEqualTo("key2Value");
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.value"))).isNull();
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.value.value1")))
              .isEqualTo("value1");
          Assertions.assertThat(attributes.get(AttributeKey.stringKey("payload.value.value2")))
              .isEqualTo("value2");
        };
    assertProcessTraceAndPayloadAttributes(traces, attributeAssertions);

    KeyValue outputMessage = testOutputTopic.readKeyValue();
    Assertions.assertThat(outputMessage.key).isEqualTo(keyPojo);
    Assertions.assertThat(outputMessage.value).isEqualTo(valuePojo);
  }


  private void assertProcessTraceAndPayloadAttributes(List<List<SpanData>> traces,
      Consumer<Attributes> attributeAssertions) {

    TracesAssert.assertThat(traces).hasSize(1)
        .hasTracesSatisfyingExactly(
            trace -> trace
                .hasSize(1)
                .hasSpansSatisfyingExactly(
                    span -> span
                        .hasKind(SpanKind.CONSUMER)
                        .hasName(inputTopic + " process")
                        .hasAttributesSatisfying(attributeAssertions)
                ));
  }

  private void testKStreamProcessSpanCapturesPayloadKeysAndValue(Serde keySerde, Serde valueSerde,
      Object key, Object value,
      String expectedKey, String expectedValue) {

    TopologyTestDriver topologyTestDriver = prepareKStreamTopology(keySerde, valueSerde);
    TestInputTopic testInputTopic = createTestInputTopic(topologyTestDriver, key, value, keySerde,
        valueSerde);
    TestOutputTopic testOutputTopic = createTestOutputTopic(topologyTestDriver, keySerde,
        valueSerde);
    testInputTopic.pipeInput(key, value);

    await().atMost(Duration.ofSeconds(5)).until(() -> !testOutputTopic.isEmpty());

    List<List<SpanData>> traces = instrumentation.waitForTraces(1);

    Consumer<Attributes> attributeAssertions =
        attributes -> {
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
              .isEqualTo(expectedKey);
          assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
              .contains(expectedValue);
          assertThat(attributes.get(AttributeKey.stringKey("payload.key")))
              .isEqualTo(expectedKey);
          assertThat(attributes.get(AttributeKey.stringKey("payload.value")))
              .isEqualTo(expectedValue);
        };

    assertProcessTraceAndPayloadAttributes(traces, attributeAssertions);

    KeyValue outputMessage = testOutputTopic.readKeyValue();
    assertThat(outputMessage.key).isEqualTo(key);
    assertThat(outputMessage.value).isEqualTo(value);
  }

  private TopologyTestDriver prepareKStreamTopology(Serde keySerde, Serde valueSerde) {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream(inputTopic, Consumed.with(keySerde, valueSerde)).mapValues(x -> x)
        .to(outputTopic, Produced.with(keySerde, valueSerde));
    return new TopologyTestDriver(streamsBuilder.build(),
        commonTestUtils.getPropertiesForStreams());
  }

  private TestInputTopic createTestInputTopic(TopologyTestDriver topologyTestDriver, Object key,
      Object value, Serde keySerde, Serde valueSerde) {
    return topologyTestDriver.createInputTopic(inputTopic, keySerde.serializer(),
        valueSerde.serializer());
  }

  private TestOutputTopic createTestOutputTopic(TopologyTestDriver topologyTestDriver,
      Serde keySerde, Serde valueSerde) {
    return topologyTestDriver.createOutputTopic(outputTopic, keySerde.deserializer(),
        valueSerde.deserializer());
  }
}

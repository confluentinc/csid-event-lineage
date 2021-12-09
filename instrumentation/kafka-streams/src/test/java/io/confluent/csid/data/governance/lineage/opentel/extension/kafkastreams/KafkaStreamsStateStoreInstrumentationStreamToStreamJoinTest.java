/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.testing.assertj.TraceAssert;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test for tracing propagation during KStream to KStream join operations that utilize StateStore
 * put and fetch.
 * <p>
 * See {@link org.apache.kafka.streams.kstream.internals.KStreamKStreamJoin.KStreamKStreamJoinProcessor#process}
 */
public class KafkaStreamsStateStoreInstrumentationStreamToStreamJoinTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String inputTopic;
  private String inputTopic2;

  private String outputTopic;

  private CommonTestUtils commonTestUtils;

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
    inputTopic = "input-topic-" + UUID.randomUUID();
    inputTopic2 = "input-topic2-" + UUID.randomUUID();

    outputTopic = "output-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }

  @Test
  @DisplayName("Test KStream with KStream Join for Integer key / value types")
  void testKStreamWithJoinedKStreamSpanCapturesPayloadForIntegerKeyValues() {

    Pair<Integer, Integer>[] topic1Messages = new Pair[]{
        Pair.of(1, 1),
        Pair.of(1, 2),
        Pair.of(2, 3)};
    Pair<Integer, Integer>[] topic2Messages = new Pair[]{
        Pair.of(1, 10),
        Pair.of(1, 20),
        Pair.of(2, 30)};
    Pair<Integer, Integer>[] outputMessages = new Pair[]{
        Pair.of(1, 11), //Topic 2 Message 1 + Topic 1 Message 1
        Pair.of(1, 12), //Topic 1 Message 2 + Topic 1 Message 1
        Pair.of(1, 21), //Topic 2 Message 1 + Topic 1 Message 1
        Pair.of(1, 22), //Topic 2 Message 2 + Topic 1 Message 2
        Pair.of(2, 33)}; //Topic 2 Message 3 + Topic 1 Message 3

    CountDownLatch streamsControlLatch = startKStreamTopologyWithTwoStreamJoin();
    injectMessages(topic1Messages, topic2Messages);
    consumeExpectedNumberOfEvents(5);
    streamsControlLatch.countDown();

    List<List<SpanData>> traces = instrumentation.waitForTraces(6);

    TracesAssert.assertThat(traces).hasSize(6)
        .hasTracesSatisfyingExactly(
            // Input topic 1 message 1 - only goes to StateStore and backing changelog.
            messageStoredIntoStateStoreOnly(topic1Messages[0]),

            // Input topic 2 message 1 - join to Input topic 1 message 1 stored into state store
            // above.
            // StateStore get, output send, StateStore put and changelog send.
            messageJoinedWithStateStoreMessageAndSentToOutput(topic2Messages[0], topic1Messages[0],
                outputMessages[0]),

            // Input topic 1 message 2 - reverse join to Input topic 2 message 1 stored into state store
            // above.
            // StateStore get, output send, StateStore put and changelog send.
            messageJoinedWithStateStoreMessageAndSentToOutput(topic1Messages[1], topic2Messages[0],
                outputMessages[1]),

            // Input topic 2 message 2 - generates 2 output messages as we have 2 message from
            // topic 1 stored in the state store already join to Input topic 1 message 1, send out,
            // join to Input topic 1 message 2, send out.
            // StateStore get, output send, StateStore get, output send, state store put and changelog send.
            messageJoinedWithStateStoreMessageAndSentToOutputTwice(topic2Messages[1],
                topic1Messages[0], outputMessages[2],
                topic1Messages[1], outputMessages[3]),

            // Input topic 1 message 3 - message with new key so again - only goes to StateStore and backing changelog.
            messageStoredIntoStateStoreOnly(topic1Messages[2]),

            // Input topic 2 message 3 - with new key - single join and output join to
            // Input topic 1 message 3 stored into state store above.
            // StateStore get, output send, StateStore put and changelog send.
            messageJoinedWithStateStoreMessageAndSentToOutput(topic2Messages[2], topic1Messages[2],
                outputMessages[4])
        );
  }


  private Consumer<TraceAssert> messageStoredIntoStateStoreOnly(
      Pair<Integer, Integer> inputMessage) {
    return trace -> trace.hasSize(4)
        .hasSpansSatisfyingExactly(
            //Input produce
            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            ),
            //Input consume
            span -> span.hasKind(SpanKind.CONSUMER).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            ),
            //StateStore PUT
            span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            ),
            //ChangeLog produce
            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            )
        );
  }

  private Consumer<TraceAssert> messageJoinedWithStateStoreMessageAndSentToOutput(
      Pair<Integer, Integer> inputMessage, Pair<Integer, Integer> stateStoreMessage,
      Pair<Integer, Integer> outputMessage) {
    return trace -> trace
        .hasSize(7)
        .hasSpansSatisfyingExactly(
            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            ), //Input produce
            span -> span.hasKind(SpanKind.CONSUMER).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            ), //Input consume
            span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                //key/value of message from topic1 stored in StateStore
                keyAndValueAttributeAssertions(stateStoreMessage)
            ), //StateStore GET

            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                // sum of message stored in StateStore and received message
                keyAndValueAttributeAssertions(outputMessage)
            ), //Output produce

            span -> span.hasKind(SpanKind.CONSUMER).hasAttributesSatisfying(
                // sum of message stored in StateStore and received message
                keyAndValueAttributeAssertions(outputMessage)
            ), //Output consume

            span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                // store value of received message from topic 2 to StateStore.
                keyAndValueAttributeAssertions(inputMessage)
            ), //StateStore PUT
            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                // send to changelog value of received message from topic 2 to StateStore.
                keyAndValueAttributeAssertions(inputMessage)
            ) //ChangeLog produce
        );
  }

  private Consumer<TraceAssert> messageJoinedWithStateStoreMessageAndSentToOutputTwice(
      Pair<Integer, Integer> inputMessage, Pair<Integer, Integer> stateStoreMessage1,
      Pair<Integer, Integer> outputMessage1, Pair<Integer, Integer> stateStoreMessage2,
      Pair<Integer, Integer> outputMessage2) {
    return trace -> trace
        .hasSize(10)
        .hasSpansSatisfyingExactly(
            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            ), //Input produce
            span -> span.hasKind(SpanKind.CONSUMER).hasAttributesSatisfying(
                keyAndValueAttributeAssertions(inputMessage)
            ), //Input consume
            span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                //key/value of message from topic1 stored in StateStore
                keyAndValueAttributeAssertions(stateStoreMessage1)
            ), //StateStore GET

            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                // sum of message stored in StateStore and received message
                keyAndValueAttributeAssertions(outputMessage1)
            ), //Output produce

            span -> span.hasKind(SpanKind.CONSUMER).hasAttributesSatisfying(
                // sum of message stored in StateStore and received message
                keyAndValueAttributeAssertions(outputMessage1)
            ), //Output consume

            span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                //key/value of message from topic1 stored in StateStore
                keyAndValueAttributeAssertions(stateStoreMessage2)
            ), //StateStore GET

            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                // sum of message stored in StateStore and received message
                keyAndValueAttributeAssertions(outputMessage2)
            ), //Output produce

            span -> span.hasKind(SpanKind.CONSUMER).hasAttributesSatisfying(
                // sum of message stored in StateStore and received message
                keyAndValueAttributeAssertions(outputMessage2)
            ), //Output consume

            span -> span.hasKind(SpanKind.INTERNAL).hasAttributesSatisfying(
                // store value of received message from topic 2 to StateStore.
                keyAndValueAttributeAssertions(inputMessage)
            ), //StateStore PUT
            span -> span.hasKind(SpanKind.PRODUCER).hasAttributesSatisfying(
                // send to changelog value of received message from topic 2 to StateStore.
                keyAndValueAttributeAssertions(inputMessage)
            ) //ChangeLog produce
        );
  }

  Consumer<Attributes> keyAndValueAttributeWithTimestampAssertions(Integer expectedKey,
      Integer expectedValue) {
    return keyAttributeAssertions(expectedKey).andThen(
        valueWithTimestampAttributeAssertions(expectedValue));
  }

  Consumer<Attributes> keyAndValueAttributeAssertions(Pair<Integer, Integer> expectedKeyValue) {
    return keyAttributeAssertions(expectedKeyValue.getKey()).andThen(
        valueAttributeAssertions(expectedKeyValue.getValue()));
  }

  private Consumer<Attributes> valueWithTimestampAttributeAssertions(int expectedValue) {
    return attributes -> {
      assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
          .contains("{\"value\":" + expectedValue + ",\"timestamp\":");

      assertThat(attributes.get(AttributeKey.stringKey("payload.value.value")))
          .isEqualTo(String.valueOf(expectedValue));
      assertThat(attributes.get(AttributeKey.stringKey("payload.value.timestamp")))
          .isNotBlank();
    };
  }

  private Consumer<Attributes> valueAttributeAssertions(int expectedValue) {
    return attributes -> {
      assertThat(attributes.get(AttributeKey.stringKey("payload.raw.value")))
          .isEqualTo(String.valueOf(expectedValue));

      assertThat(attributes.get(AttributeKey.stringKey("payload.value")))
          .isEqualTo(String.valueOf(expectedValue));
    };
  }

  private Consumer<Attributes> keyAttributeAssertions(Integer expectedKey) {
    return attributes -> {
      assertThat(attributes.get(AttributeKey.stringKey("payload.raw.key")))
          .isEqualTo(expectedKey.toString());

      assertThat(attributes.get(AttributeKey.stringKey("payload.key")))
          .isEqualTo(expectedKey.toString());
    };
  }

  private CountDownLatch startKStreamTopologyWithTwoStreamJoin() {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    Properties properties = commonTestUtils.getPropertiesForStreams();
    // Create topology
    {
      KStream<Integer, Integer> stream1 = streamsBuilder.stream(inputTopic,
          Consumed.with(Serdes.Integer(), Serdes.Integer()));
      KStream<Integer, Integer> stream2 = streamsBuilder.stream(inputTopic2,
          Consumed.with(Serdes.Integer(), Serdes.Integer()));
      stream1.join(stream2, (ValueJoiner<Integer, Integer, Long>) Long::sum,
              JoinWindows.of(Duration.ofMillis(200)))
          .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Long()));
    }

    KafkaStreams kafkaStreams;
    // Create Kafka streams
    {
      properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
          Serdes.Integer().getClass().getName());
      properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
          Serdes.Integer().getClass().getName());

      kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
    }
    // Create topics
    {
      AdminClient adminClient = KafkaAdminClient.create(
          commonTestUtils.getKafkaProperties(new Properties()));
      adminClient.createTopics(Arrays.asList(
          new NewTopic(inputTopic, 1, (short) 1),
          new NewTopic(inputTopic2, 1, (short) 1),
          new NewTopic(outputTopic, 1, (short) 1)));
    }

    //Start kafka streams and return control latch
    {
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
      return streamsLatch;
    }
  }

  private void injectMessages(Pair<Integer, Integer>[] topic1Messages,
      Pair<Integer, Integer>[] topic2Messages) {
    for (int i = 0; i < topic1Messages.length; i++) {
      commonTestUtils.produceSingleEvent(Serdes.Integer().serializer().getClass(),
          Serdes.Integer().serializer().getClass(), inputTopic, topic1Messages[i].getKey(),
          topic1Messages[i].getValue());

      commonTestUtils.produceSingleEvent(Serdes.Integer().serializer().getClass(),
          Serdes.Integer().serializer().getClass(), inputTopic2, topic2Messages[i].getKey(),
          topic2Messages[i].getValue());
    }
  }

  private void consumeExpectedNumberOfEvents(int numberOfEventsToConsume) {
    commonTestUtils.consumeAtLeastXEvents(Serdes.Integer().deserializer().getClass(),
        Serdes.Long().deserializer().getClass(), outputTopic, numberOfEventsToConsume);
  }

}


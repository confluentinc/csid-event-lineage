/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER_2;
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
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
    inputTopic2 = "input-topic2-" + UUID.randomUUID();

    outputTopic = "output-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }

  @Test
  @DisplayName("Test KStream with KStream Join with header propagation and capture")
  void testKStreamWithJoinedKStreamHeaderPropagationAndCapture() {
    Header msg1ExpectedHeader = CAPTURED_PROPAGATED_HEADER;
    Header msg2ExpectedHeader = CAPTURED_PROPAGATED_HEADER_2;
    Header msg1UnexpectedHeader = NOT_WHITELISTED_HEADERS[0];
    Header msg2UnexpectedHeader = NOT_WHITELISTED_HEADERS[1];

    Triple<Integer, Integer, Header[]>[] topic1Messages = new Triple[]{
        Triple.of(1, 1,
            headers(
                msg1ExpectedHeader,
                msg1UnexpectedHeader)
        ),
        Triple.of(1, 2, new Header[0]),
        Triple.of(2, 3, new Header[0])};

    Triple<Integer, Integer, Header[]>[] topic2Messages = new Triple[]{
        Triple.of(1, 10,
            headers(
                msg2ExpectedHeader,
                msg2UnexpectedHeader)
        ),
        Triple.of(1, 20, new Header[0]),
        Triple.of(2, 30, new Header[0])};

    Triple<Integer, Integer, Header[]>[] expectedOutputMessages = new Triple[]{
        //Topic 1 Message 1 + Topic 2 Message 1
        Triple.of(1, 11,
            headers(
                msg1ExpectedHeader,
                msg2ExpectedHeader)
        ),
        //Topic 1 Message 2 + Topic 2 Message 1
        Triple.of(1, 12,
            headers(
                msg1ExpectedHeader,
                msg2ExpectedHeader)
        ),
        //Topic 1 Message 1 + Topic 2 Message 2
        Triple.of(1, 21,
            headers(
                msg1ExpectedHeader
            )
        ),
        //Topic 1 Message 2 + Topic 2 Message 2
        Triple.of(1, 22,
            headers(
                msg1ExpectedHeader,
                msg2ExpectedHeader)
        ),
        //Topic 1 Message 3 + Topic 2 Message 3
        Triple.of(2, 33, headers())
    };

    CountDownLatch streamsControlLatch = startKStreamTopologyWithTwoStreamJoin();
    injectMessages(topic1Messages, topic2Messages);
    verifyConsumedOutputEvents(expectedOutputMessages);
    streamsControlLatch.countDown();

    List<List<SpanData>> traces = instrumentation.waitForTraces(6);
    assertTracesCaptured(traces,
        trace().withSpans(
            produce().withHeaders(CHARSET_UTF_8, msg1ExpectedHeader),
            consume(),
            //T1 M1
            stateStorePut().withNameContaining("KSTREAM-JOINTHIS")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader)
                .withoutHeaders(msg1UnexpectedHeader),
            produceChangelog().withNameContaining("KSTREAM-JOINTHIS")
                .withoutHeaders(msg1ExpectedHeader, msg1UnexpectedHeader)),
        trace().withSpans(
            produce(),
            consume(),
            stateStoreGet()
                .withLink()
                .withNameContaining("KSTREAM-JOINTHIS")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)
                .withoutHeaders(msg2ExpectedHeader),
            //Output 1 - T1 M1(from state store) + T2 M1 (from inbound stream)
            produce().withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            consume().withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            //T2 M1
            stateStorePut()
                .withNameContaining("KSTREAM-JOINOTHER")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader)
                .withoutHeaders(NOT_WHITELISTED_HEADERS),
            produceChangelog()
                .withNameContaining("KSTREAM-JOINOTHER")
                .withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader)
                .withoutHeaders(NOT_WHITELISTED_HEADERS)),
        trace().withSpans(
            produce().withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            consume(),
            stateStoreGet().withLink().withNameContaining("KSTREAM-JOINOTHER")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader),
            //Output 2 - T1 M2(from inbound stream) + T2 M1 (from state store)
            produce().withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader),
            consume(),
            // T1 M2
            stateStorePut().withNameContaining("KSTREAM-JOINTHIS")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader),
            produceChangelog().withNameContaining("KSTREAM-JOINTHIS")),
        trace().withSpans(
            produce().withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            consume(),
            stateStoreGet().withLink().withNameContaining("KSTREAM-JOINTHIS")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader),
            //Output 3 - T1 M1(from state store) + T2 M2 (from inbound stream)
            produce().withHeaders(CHARSET_UTF_8, msg1ExpectedHeader),
            consume(),
            stateStoreGet().withLink().withNameContaining("KSTREAM-JOINTHIS")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader),
            //Output 4 - T1 M2(from state store) + T2 M2 (from inbound stream)
            produce().withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader),
            consume(),
            stateStorePut().withNameContaining("KSTREAM-JOINOTHER")
                .withHeaders(CHARSET_UTF_8, msg1ExpectedHeader, msg2ExpectedHeader),
            produceChangelog().withNameContaining("KSTREAM-JOINOTHER")),
        trace().withSpans(
            produce().withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            consume(),
            stateStorePut().withNameContaining("KSTREAM-JOINTHIS")
                .withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            produceChangelog().withNameContaining("KSTREAM-JOINTHIS")),
        trace().withSpans(
            produce().withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            consume(),
            stateStoreGet().withLink().withNameContaining("KSTREAM-JOINTHIS")
                .withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            //Output 5 - T1 M3(from state store) + T2 M3 (from inbound stream)
            produce().withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            consume(),
            stateStorePut().withNameContaining("KSTREAM-JOINOTHER")
                .withoutHeaders(msg1ExpectedHeader, msg2ExpectedHeader),
            produceChangelog().withNameContaining("KSTREAM-JOINOTHER")));
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
      stream1.join(stream2, (ValueJoiner<Integer, Integer, Integer>) Integer::sum,
              JoinWindows.of(Duration.ofMillis(1000)))
          .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Integer()));
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
          kafkaStreams.cleanUp();
        } catch (InterruptedException e) {
          kafkaStreams.close();
          kafkaStreams.cleanUp();
          Thread.currentThread().interrupt();
        }
      }).start();
      return streamsLatch;
    }
  }

  private void injectMessages(Triple<Integer, Integer, Header[]>[] topic1Messages,
      Triple<Integer, Integer, Header[]>[] topic2Messages) {
    for (int i = 0; i < topic1Messages.length; i++) {
      commonTestUtils.produceSingleEvent(inputTopic, topic1Messages[i].getLeft(),
          topic1Messages[i].getMiddle(), topic1Messages[i].getRight());

      commonTestUtils.produceSingleEvent(inputTopic2, topic2Messages[i].getLeft(),
          topic2Messages[i].getMiddle(), topic2Messages[i].getRight());
    }
  }

  private void verifyConsumedOutputEvents(
      Triple<Integer, Integer, Header[]>[] expectedOutputEvents) {
    List<ConsumerRecord> consumedRecords = commonTestUtils.consumeAtLeastXEvents(
        Serdes.Integer().deserializer().getClass(),
        Serdes.Integer().deserializer().getClass(), outputTopic, expectedOutputEvents.length);
    IntStream.range(0, consumedRecords.size())
        .forEach(idx -> assertConsumedRecord(consumedRecords.get(idx), expectedOutputEvents[idx]));
  }

  private void assertConsumedRecord(ConsumerRecord consumedRecord,
      Triple<Integer, Integer, Header[]> expectation) {
    assertThat(consumedRecord.key()).isEqualTo(expectation.getLeft());
    assertThat(consumedRecord.value()).isEqualTo(expectation.getMiddle());
    assertThat(consumedRecord.headers()).containsAll(Arrays.asList(expectation.getRight()));
  }

  private Header[] headers(Header... headers) {
    return headers;
  }
}


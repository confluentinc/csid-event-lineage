/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;


import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.CommonTestUtils.assertTracesCaptured;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.SpanAssertData.consume;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.SpanAssertData.produce;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.TraceAssertData.trace;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanSuppressionConfiguration.SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ConfigurationReloader;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class ConsumerInterceptorSuppressionTest {

  @RegisterExtension
  static final AgentInstrumentationExtension instrumentation =
      AgentInstrumentationExtension.create();

  private String testTopic;

  private CommonTestUtils commonTestUtils;

  @BeforeEach
  void setup() {
    commonTestUtils = new CommonTestUtils();
    commonTestUtils.startKafkaContainer();
    testTopic = "test-topic-" + UUID.randomUUID();
  }

  @AfterEach
  void teardown() {
    commonTestUtils.stopKafkaContainer();
    instrumentation.clearData();
  }

  @Test
  @DisplayName("Test consumer process span is not recorded by iterating inside interceptors when all interceptors disabled by config")
  void testConsumerProcessSpanSuppressedForInterceptorsWhenAllInterceptorDisabled() {
    System.setProperty(SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP, "*");
    ConfigurationReloader.reloadSpanSuppressionConfiguration(
        Singletons.spanSuppressionConfiguration());

    String key = "key";
    String value = "value";

    commonTestUtils.produceSingleEvent(testTopic, key, value);
    commonTestUtils.updateAdditionalProperties(
        props -> props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            String.join(",", TestConsumerInterceptor.class.getName(),
                TestConsumerInterceptorAnother.class.getName())));
    commonTestUtils.consumeEvent(testTopic);
    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    //1 Consumer spans as both interceptors should be disabled.
    assertTracesCaptured(traces, trace().withSpans(produce(), consume()));

    System.clearProperty(SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP);
    ConfigurationReloader.reloadSpanSuppressionConfiguration(
        Singletons.spanSuppressionConfiguration());
  }

  @Test
  @DisplayName("Test consumer process span is not recorded by iterating inside interceptor when specific interceptor disabled by config")
  void testConsumerProcessSpanSuppressedOnlyForInterceptorThatIsDisabled() {
    System.setProperty(SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP,
        TestConsumerInterceptor.class.getSimpleName());
    ConfigurationReloader.reloadSpanSuppressionConfiguration(
        Singletons.spanSuppressionConfiguration());

    String key = "key";
    String value = "value";

    commonTestUtils.produceSingleEvent(testTopic, key, value);
    commonTestUtils.updateAdditionalProperties(
        props -> props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            String.join(",", TestConsumerInterceptor.class.getName(),
                TestConsumerInterceptorAnother.class.getName())));
    commonTestUtils.consumeEvent(testTopic);
    List<List<SpanData>> traces = instrumentation.waitForTraces(1);
    //2 Consumer spans as one interceptor should be disabled while another one still enabled.
    assertTracesCaptured(traces, trace().withSpans(produce(), consume(), consume()));

    System.clearProperty(SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP);
    ConfigurationReloader.reloadSpanSuppressionConfiguration(
        Singletons.spanSuppressionConfiguration());
  }
}


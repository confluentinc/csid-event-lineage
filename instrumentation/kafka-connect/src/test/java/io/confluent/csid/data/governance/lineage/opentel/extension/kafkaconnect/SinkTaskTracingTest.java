/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER_2;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SinkTaskTracingTest extends IntegrationTest{

  private final String SINK_TASK_NAME = String.format(SpanNames.TASK_SPAN_NAME_FORMAT, testTopic,
      SpanNames.SINK_TASK);
  private final String CONSUMER_PROCESS_TASK_NAME = String.format(SpanNames.PRODUCE_CONSUME_TASK_FORMAT, testTopic,
      SpanNames.CONSUMER_PROCESS);
  private final String SMT_TASK_NAME = String.format(SpanNames.SMT_SPAN_NAME_FORMAT, SpanNames.SMT,
      transformClassName);

  private final Charset charset = StandardCharsets.UTF_8;

  @BeforeEach
  void setup() {
    super.setup();
    startConnectContainer(Connectors.SINK_WITH_SMT);
  }

  @AfterEach
  void cleanup() {
    super.cleanup();
  }

  @Test
  void testSinkTaskCaptureWithHeaderPropagationAndCapture() {

    String key = " {\"schema\":{\"type\":\"int32\",\"optional\":false},\"payload\":0}";
    String value = "{\"schema\":{\"type\":\"int64\",\"optional\":false},\"payload\":31}";
    produceSingleEvent(testTopic, key, value, CAPTURED_PROPAGATED_HEADER_2);

    //Looking for trace with smt, sink spans.
    List<Pair<Resource, Span>> expectedTrace = findTraceBySpanNamesWithinTimeout(10,
        CONSUMER_PROCESS_TASK_NAME, SMT_TASK_NAME, SINK_TASK_NAME);

    assertThat(expectedTrace).as("Could not find trace with %s, %s, %s spans.",CONSUMER_PROCESS_TASK_NAME, SMT_TASK_NAME, SINK_TASK_NAME)
        .isNotNull();
    assertThat(expectedTrace.size()).as("Unexpected span as part of %s, %s, %s trace.",CONSUMER_PROCESS_TASK_NAME, SMT_TASK_NAME, SINK_TASK_NAME)
        .isEqualTo(3);


    //All 3 spans should have header sent by producer captured / propagated
    expectedTrace.forEach(
        resourceSpanPair -> assertSpanAttribute(resourceSpanPair.getRight(),
            "headers." + CAPTURED_PROPAGATED_HEADER_2.key(),
            new String(CAPTURED_PROPAGATED_HEADER_2.value(), charset)));

    //SMT and SINK spans should have header set by SMT captured / propagated
    List<Pair<Resource, Span>> smtAndSinkSpans = filterSpansBySpanNames(expectedTrace, SMT_TASK_NAME, SINK_TASK_NAME);
    smtAndSinkSpans.forEach(
        resourceSpanPair -> assertSpanAttribute(resourceSpanPair.getRight(),
            "headers." + CAPTURED_PROPAGATED_HEADER.key(),
            new String(CAPTURED_PROPAGATED_HEADER.value(), charset)));
  }
}

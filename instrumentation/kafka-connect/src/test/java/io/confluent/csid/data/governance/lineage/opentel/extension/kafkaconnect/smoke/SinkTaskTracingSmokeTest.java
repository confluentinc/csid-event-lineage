/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke.IntegrationTestBase.Connectors.SINK_CONNECTOR_NAME;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke.TraceAssertUtils.assertSpanHasAttribute;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke.TraceAssertUtils.assertSpanHasExpectedClusterId;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke.TraceAssertUtils.assertSpanHasExpectedServiceName;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER_2;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CHARSET_UTF_8;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TestConstants.TIMEOUTS.DEFAULT_TIMEOUT_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SinkTaskTracingSmokeTest extends IntegrationTestBase {

  private final String SINK_TASK_NAME = String.format(SpanNames.TASK_SPAN_NAME_FORMAT, testTopic,
      SpanNames.SINK_TASK);
  private final String CONSUMER_PROCESS_TASK_NAME = String.format(
      SpanNames.PRODUCE_CONSUME_TASK_FORMAT, testTopic,
      SpanNames.CONSUMER_PROCESS);
  private final String SMT_TASK_NAME = String.format(SpanNames.SMT_SPAN_NAME_FORMAT, SpanNames.SMT,
      transformClassName);

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
    commonTestUtils.produceSingleEvent(testTopic, key, value, CAPTURED_PROPAGATED_HEADER_2);

    //Looking for trace with consumer process, smt, sink spans.
    List<Pair<Resource, Span>> expectedTrace = traceAssertUtils.findTraceBySpanNamesWithinTimeout(
        DEFAULT_TIMEOUT_SECONDS,
        CONSUMER_PROCESS_TASK_NAME, SMT_TASK_NAME, SINK_TASK_NAME);

    assertThat(expectedTrace).as("Could not find trace with %s, %s, %s spans.",
            CONSUMER_PROCESS_TASK_NAME, SMT_TASK_NAME, SINK_TASK_NAME)
        .isNotNull();
    assertThat(expectedTrace.size()).as("Unexpected span as part of %s, %s, %s trace.",
            CONSUMER_PROCESS_TASK_NAME, SMT_TASK_NAME, SINK_TASK_NAME)
        .isEqualTo(3);

    //All 3 spans should have header sent by producer captured / propagated
    expectedTrace.forEach(
        resourceSpanPair -> assertSpanHasAttribute(resourceSpanPair.getRight(),
            "headers." + CAPTURED_PROPAGATED_HEADER_2.key(),
            new String(CAPTURED_PROPAGATED_HEADER_2.value(), CHARSET_UTF_8)));

    //SMT and SINK spans should have header set by SMT captured / propagated
    List<Pair<Resource, Span>> smtAndSinkSpans = traceAssertUtils.filterSpansBySpanNames(
        expectedTrace,
        SMT_TASK_NAME, SINK_TASK_NAME);
    smtAndSinkSpans.forEach(
        resourceSpanPair -> assertSpanHasAttribute(resourceSpanPair.getRight(),
            "headers." + CAPTURED_PROPAGATED_HEADER.key(),
            new String(CAPTURED_PROPAGATED_HEADER.value(), CHARSET_UTF_8)));

    //All 3 spans should have service.name Resource attribute = Connector name
    expectedTrace.forEach(
        resourceSpanPair -> assertSpanHasExpectedServiceName(resourceSpanPair,
            SINK_CONNECTOR_NAME));

    //Verify Cluster id set on all 3 spans - as it should propagate from Consume span.
    String clusterId = commonTestUtils.getClusterId();
    expectedTrace.forEach(
        resourceSpanPair -> assertSpanHasExpectedClusterId(resourceSpanPair, clusterId));
  }
}

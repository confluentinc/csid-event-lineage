/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.HeaderPropagationTestUtils.CAPTURED_PROPAGATED_HEADER;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SmtTracingTest extends IntegrationTest {

  private final Charset charset = StandardCharsets.UTF_8;
  private final String SOURCE_TASK_NAME = String.format(SpanNames.TASK_SPAN_NAME_FORMAT, testTopic,
      SpanNames.SOURCE_TASK);

  private final String SMT_TASK_NAME = String.format(SpanNames.SMT_SPAN_NAME_FORMAT, SpanNames.SMT,
      transformClassName);

  private final String SEND_TASK_NAME = String.format(SpanNames.PRODUCE_CONSUME_TASK_FORMAT,
      testTopic, SpanNames.PRODUCER_SEND);

  @BeforeEach
  void setup() {
    super.setup();
    startConnectContainer(Connectors.SOURCE_WITH_SMT);
  }

  @AfterEach
  void cleanup() {
    super.cleanup();
  }

  @Test
  void testSMTCaptureWithHeaderCapture() {

    consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
        testTopic, 1);

    //Looking for trace with source, smt, send spans.
    List<Pair<Resource, Span>> expectedTrace = findTraceBySpanNamesWithinTimeout(10,
        SOURCE_TASK_NAME, SMT_TASK_NAME, SEND_TASK_NAME);

    assertThat(expectedTrace).as("Could not find trace with %s, %s, %s spans.", SOURCE_TASK_NAME,
            SMT_TASK_NAME, SEND_TASK_NAME)
        .isNotNull();
    assertThat(expectedTrace.size()).as("Unexpected span as part of %s, %s, %s trace.",
            SOURCE_TASK_NAME, SMT_TASK_NAME, SEND_TASK_NAME)
        .isEqualTo(3);

    //Check that SMT span and Producer Send span has headers captured / propagated.
    //Source task won't have that header as it's injected by SMT.
    List<Pair<Resource, Span>> spansWithHeaders = filterSpansBySpanNames(expectedTrace,
        SMT_TASK_NAME, SEND_TASK_NAME);
    assertThat(spansWithHeaders.size()).as("Expected 2 spans - SMT and Send").isEqualTo(2);
    spansWithHeaders.forEach(
        resourceSpanPair -> assertSpanAttribute(resourceSpanPair.getRight(),
            "headers." + CAPTURED_PROPAGATED_HEADER.key(),
            new String(CAPTURED_PROPAGATED_HEADER.value(), charset)));
  }

}

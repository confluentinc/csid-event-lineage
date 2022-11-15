/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.smoke;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils.TestConstants.TIMEOUTS.DEFAULT_TIMEOUT_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SourceTaskTracingSmokeTest extends IntegrationTestBase {

  private String testTopic = "connect-topic";
  private final String SOURCE_TASK_NAME = String.format(SpanNames.TASK_SPAN_NAME_FORMAT, testTopic,
      SpanNames.SOURCE_TASK);
  private final String SEND_TASK_NAME = String.format(SpanNames.PRODUCE_CONSUME_TASK_FORMAT,
      testTopic, SpanNames.PRODUCER_SEND);

  @BeforeEach
  void setup() {
    super.setup();
    startConnectContainer(Connectors.SOURCE_NO_SMT);
  }

  @AfterEach
  void cleanup() {
    super.cleanup();
  }

  @Test
  void testSourceTask() {

    commonTestUtils.consumeAtLeastXEvents(StringDeserializer.class, StringDeserializer.class,
        testTopic, 1);

    //Looking for trace with sourceTask -> send task spans.
    List<Pair<Resource, Span>> expectedTrace = traceAssertUtils.findTraceBySpanNamesWithinTimeout(DEFAULT_TIMEOUT_SECONDS,
        SOURCE_TASK_NAME, SEND_TASK_NAME);
    assertThat(expectedTrace).as("Could not find trace with %s, %s spans.", SOURCE_TASK_NAME,
            SEND_TASK_NAME)
        .isNotNull();
    assertThat(expectedTrace.size()).as("Unexpected span as part of %s, %s trace.",
            SOURCE_TASK_NAME, SEND_TASK_NAME)
        .isEqualTo(2);
  }


}

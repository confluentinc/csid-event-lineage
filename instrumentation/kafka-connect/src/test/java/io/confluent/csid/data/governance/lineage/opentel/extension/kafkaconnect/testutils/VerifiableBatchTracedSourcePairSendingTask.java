/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Extension of {@link VerifiableSourcePairSendingTask} - sends 2 events on first poll and empty
 * results on subsequent polls to reduce unnecessary trace / span noise in tests.
 * <p>
 * Additionally, creates Span per whole batch of records created during poll() execution.
 * <p>
 * That behaviour simulates Source Connector with tracing support on per poll() / result batch
 * basis
 *
 * @see VerifiableSourcePairSendingTask
 * @see VerifiableSourceBatchTracedConnector
 */
public class VerifiableBatchTracedSourcePairSendingTask extends
    VerifiableSourcePairSendingTask {

  public static final String SPAN_NAME = "test-source-process";


  /**
   * main method of the Source Task - on first call - generates and returns 2 events, on subsequent
   * calls returns empty result list to reduce unnecessary trace / span noise in tests.
   * <p>
   * Creates single trace and span per batch of records returned.
   */
  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long sendStartMs = System.currentTimeMillis();
    if (throttler.shouldThrottle(seqno - startingSeqno, sendStartMs)) {
      throttler.throttle();
    }
    List<SourceRecord> result = new ArrayList<>();
    if (sent) {
      return result;
    }
    //Simulating source task that has span created per poll/batch of records.
    SpanBuilder spanBuilder = GlobalOpenTelemetry
        .getTracer("test-instrumentation")
        .spanBuilder(SPAN_NAME)
        .setParent(Context.current());
    Span span = spanBuilder.startSpan();
    try (Scope ignored = span.makeCurrent()) {
      for (int i = 0; i < NUMBER_OF_EVENTS_TO_GENERATE; i++) {

        Map<String, Long> ccOffset = Collections.singletonMap(SEQNO_FIELD, seqno);
        SourceRecord srcRecord = new SourceRecord(partition, ccOffset, topic,
            Schema.INT32_SCHEMA, id, Schema.INT64_SCHEMA, seqno);
        result.add(srcRecord);
        seqno++;
        span.end();
      }
    }
    sent = true;
    return result;
  }
}

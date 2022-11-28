/*
 * Copyright 2022 Confluent Inc.
 */
package io.opentelemetry.sdk.testing.assertj;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.SpanAssertData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class TraceAssertData implements Consumer<List<SpanData>> {

  List<SpanAssertData> spanAsserts;
  List<SpanData> actual;

  @Override
  public void accept(List<SpanData> actual) {
    this.actual = actual;
    TraceAssert traceAssert = new TraceAssert(actual);
    traceAssert.as("Trace %s should have %d spans", actual.get(0).getTraceId(), spanAsserts.size())
        .hasSize(spanAsserts.size());
    for (int i = 0; i < spanAsserts.size(); i++) {
      spanAsserts.get(i).accept(new SpanDataAssert(traceAssert.getSpan(i)), traceAssert.getSpan(i));
    }
  }

  public static TraceAssertData trace() {
    return new TraceAssertData();
  }

  public TraceAssertData withSpans(SpanAssertData... spanAssertions) {
    this.spanAsserts = Arrays.asList(spanAssertions);
    return this;
  }
}

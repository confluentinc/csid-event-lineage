/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import io.opentelemetry.sdk.testing.assertj.SpanDataAssert;
import io.opentelemetry.sdk.testing.assertj.TraceAssert;
import java.util.function.Consumer;

public class TraceAssertData implements Consumer<TraceAssert> {

  Consumer<SpanDataAssert>[] spanAsserts;

  @Override
  public void accept(TraceAssert tracesAssert) {
    tracesAssert
        .hasSize(spanAsserts.length)
        .hasSpansSatisfyingExactly(spanAsserts);
  }

  public static TraceAssertData trace() {
    return new TraceAssertData();
  }

  public TraceAssertData withSpans(Consumer<SpanDataAssert>... spanAssertions) {
    this.spanAsserts = spanAssertions;
    return this;
  }
}

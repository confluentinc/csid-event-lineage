/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_HEADER;
import static java.util.Collections.singletonList;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;


/**
 * Handles retrieval of TraceId from String parsed from StateStore stored value. See
 * {@link TextMapPropagator} and {@link TextMapGetter}
 */
public class StringTextMapGetter implements TextMapGetter<String> {

  private static final StringTextMapGetter instance = new StringTextMapGetter();

  public static StringTextMapGetter getInstance() {
    return instance;
  }

  @Override
  public Iterable<String> keys(String carrier) {
    return singletonList(TRACING_HEADER);
  }

  @Override
  public String get(String carrier, String key) {
    if (TRACING_HEADER.equals(key)) {
      return carrier;
    } else {
      return null;
    }
  }
}

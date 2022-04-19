/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.TRACING_HEADER;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * Handles creation of String with TraceId header - to be subsequently appended to value being
 * stored into state store. See {@link TextMapPropagator} and {@link TextMapGetter}
 */
public class StringTextMapSetter implements TextMapSetter<String[]> {

  private static final StringTextMapSetter instance = new StringTextMapSetter();

  public static StringTextMapSetter getInstance() {
    return instance;
  }

  @Override
  public void set(String[] carrier, String key, String value) {
    if (TRACING_HEADER.equals(key)) {
      carrier[0] = value;
    }
  }
}

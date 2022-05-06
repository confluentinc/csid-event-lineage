/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.Value;
import org.apache.kafka.common.header.Header;

/**
 * Headers holder for capturing and propagating headers in Kafka Consumer / Producer.
 * <p>
 * Whitelisted set of headers are stored into the holder on Consume - iterate and subsequently
 * propagated on Produce (given its thread local - Consume - Produce process has to be thread
 * bound).
 */
@Value
public class HeadersHolder {

  private static final ThreadLocal<Header[]> HEADERS_HOLDER = new ThreadLocal<>();

  public static void store(Header[] headers) {
    HEADERS_HOLDER.set(headers);
  }

  public static Header[] get() {
    return HEADERS_HOLDER.get();
  }

  public static void clear() {
    HEADERS_HOLDER.remove();
  }
}

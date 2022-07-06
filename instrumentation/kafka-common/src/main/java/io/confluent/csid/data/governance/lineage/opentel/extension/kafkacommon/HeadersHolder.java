/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import java.util.Optional;
import lombok.Value;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Headers holder for capturing and propagating headers in Kafka Consumer / Producer.
 * <p>
 * Whitelisted set of headers are stored into the holder on Consume - iterate and subsequently
 * propagated on Produce (given its thread local - Consume - Produce process has to be thread
 * bound).
 */
@Value
public class HeadersHolder {

  private static final ThreadLocal<Headers> HEADERS_HOLDER = new ThreadLocal<>();

  public static void store(Headers headers) {
    HEADERS_HOLDER.set(headers);
  }

  public static void store(Header[] headers) {
    HEADERS_HOLDER.set(new RecordHeaders(headers));
  }

  /**
   * Null safe headers' retrieval - removes the need for null checking in all downstream logic.
   * @return Headers stored or empty Headers if none. Null-safe.
   */
  public static Headers get() {
    return Optional.ofNullable(HEADERS_HOLDER.get()).orElse(new RecordHeaders());
  }

  public static void clear() {
    HEADERS_HOLDER.remove();
  }
}

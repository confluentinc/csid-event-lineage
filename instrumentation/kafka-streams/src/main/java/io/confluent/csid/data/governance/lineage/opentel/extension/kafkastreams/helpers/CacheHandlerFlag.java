/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import java.util.Optional;
import lombok.Value;

@Value
public class CacheHandlerFlag {

  private static final ThreadLocal<Boolean> CACHE_HANDLER_FLAG = new ThreadLocal<>();

  public static void enable() {
    CACHE_HANDLER_FLAG.set(true);
  }

  public static void disable() {
    CACHE_HANDLER_FLAG.set(false);
  }

  public static boolean isEnabled() {
    return Optional.ofNullable(CACHE_HANDLER_FLAG.get()).orElse(false);
  }
}

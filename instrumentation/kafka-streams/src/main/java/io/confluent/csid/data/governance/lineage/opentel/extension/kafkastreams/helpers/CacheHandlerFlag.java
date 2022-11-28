/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import java.util.Optional;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.apache.kafka.streams.state.internals.CachingStoreInstrumentation;
import org.apache.kafka.streams.state.internals.DirtyEntryInstrumentation;
import org.apache.kafka.streams.state.internals.LRUCacheEntryInstrumentation;


/**
 * Flag indicating if cache specific tracing logic should be performed - indicates that hashCode in
 * {@link org.apache.kafka.streams.state.internals.LRUCacheEntry} should be overridden to return
 * identityHashCode and payload should be restored when accessing
 * {@link org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntry}.
 *
 * @see CachingStoreInstrumentation
 * @see DirtyEntryInstrumentation
 * @see LRUCacheEntryInstrumentation
 */
@Value
@UtilityClass
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

/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.CACHE_LAYER;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHolder;
import java.util.function.Supplier;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * Base class for Tracing State Store wrappers providing common behavior.
 *
 * @param <T> Type of StateStore
 */
public class BaseTracingStore<T extends StateStore> extends
    WrappedStateStore<T, Bytes, byte[]> {

  /**
   * Accessor to headers for use during state store operations that require header propagation /
   * capture logic.
   */
  protected Supplier<Headers> headersAccessor = HeadersHolder::get;

  /**
   * State store can be wrapped at 2 levels - Caching layer and underlying state store layer (i.e.
   * RocksDB store)
   * <p>
   * This flag indicates whether this wrapping store is attached to Cache layer or not
   */
  protected CACHE_LAYER isCachingStore;

  public BaseTracingStore(T wrapped, CACHE_LAYER isCachingStore) {
    super(wrapped);
    this.isCachingStore = isCachingStore;
  }
}

package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import java.util.function.Supplier;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * Base class for Tracing State Store wrappers providing common behavior.
 *
 * @param <T>
 */
public class BaseTracingStore<T extends StateStore> extends
    WrappedStateStore<T, Bytes, byte[]> {

  /**
   * Accessor to headers for use during state store operations that require header propagation /
   * capture logic.
   */
  protected Supplier<Headers> headersAccessor = HeadersHolder::get;

  public BaseTracingStore(T wrapped) {
    super(wrapped);
  }
}

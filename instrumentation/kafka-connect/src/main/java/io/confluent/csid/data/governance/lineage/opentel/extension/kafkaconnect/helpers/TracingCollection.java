/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import java.util.Collection;
import java.util.Iterator;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * Wrapper class providing iterator with tracing for ConnectRecord classes - used in instrumenting
 * Connect sink / source tasks as consumer records are first converted to internal connect records
 * as a batch and only then iterated - hence breaking span scope from initial consumer records
 * iteration.
 */
@Slf4j
public class TracingCollection<T extends ConnectRecord<T>> implements Collection<T> {

  @Delegate(excludes = Iterable.class)
  private final Collection<T> delegate;
  protected final String spanName;
  protected final String connectorId;

  public TracingCollection(Collection<T> delegate, String spanName, String connectorId) {
    this.spanName = spanName;
    this.delegate = delegate;
    this.connectorId = connectorId;
  }

  @Override
  public Iterator<T> iterator() {
    return new TracingIterator<>(delegate.iterator(), spanName, connectorId);
  }
}

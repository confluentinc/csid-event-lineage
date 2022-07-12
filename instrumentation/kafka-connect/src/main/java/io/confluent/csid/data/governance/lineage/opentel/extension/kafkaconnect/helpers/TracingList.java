/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectRecord;


/**
 * Extension on TracingCollection, returns TracingIterator for listIterator() calls and TracingList
 * for subList().
 * <p>
 * Note: get() method does not trigger tracing logic / span creation.
 */
@Slf4j
public class TracingList<T extends ConnectRecord<T>> extends TracingCollection<T> implements
    List<T> {

  private interface DelegateExcludes {

    ListIterator<?> listIterator();

    ListIterator<?> listIterator(int index);

    List<?> subList(int fromIndex, int toIndex);

  }

  @Delegate(excludes = {Collection.class, DelegateExcludes.class})
  private final List<T> delegate;

  /**
   * Wraps delegate List
   *
   * @param delegate    list to wrap
   * @param spanName    Span name to pass into iterator - for creating new spans on next() call.
   * @param connectorId connectorId - used for overriding service name.
   */
  public TracingList(List<T> delegate, String spanName, String connectorId) {
    super(delegate, spanName, connectorId);
    this.delegate = delegate;
    log.trace("Creating TracingList spanName={}, delegate={}, connectorId={}", spanName, delegate,
        connectorId);

  }

  @Override
  public ListIterator<T> listIterator() {
    return new TracingListIterator<>(delegate.listIterator(), spanName, connectorId);
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    return new TracingListIterator<>(delegate.listIterator(index), spanName, connectorId);
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return new TracingList<>(delegate.subList(fromIndex, toIndex), spanName, connectorId);
  }

}

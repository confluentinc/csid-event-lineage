/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import java.util.ListIterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * Extends TracingIterator - Wraps ConnectRecord iterator and executes span creation and header
 * capture logic on next() / previous() calls.
 */
@Slf4j
public class TracingListIterator<T extends ConnectRecord<T>> extends TracingIterator<T>
    implements ListIterator<T> {

  private final ListIterator<T> delegateIterator;

  public TracingListIterator(
      ListIterator<T> delegateIterator, String spanName) {
    super(delegateIterator, spanName);
    this.delegateIterator = delegateIterator;
    log.trace("Creating TracingListIterator spanName={}, delegate={}", spanName, delegateIterator);
  }


  @Override
  public boolean hasPrevious() {
    closeScopeAndEndSpan();
    return delegateIterator.hasPrevious();
  }

  @Override
  public T previous() {
    closeScopeAndEndSpan();
    T record = delegateIterator.previous();
    recordSpan(record);
    return record;
  }

  @Override
  public int nextIndex() {
    return delegateIterator.nextIndex();
  }

  @Override
  public int previousIndex() {
    return delegateIterator.previousIndex();
  }

  @Override
  public void set(T t) {
    delegateIterator.set(t);
  }

  @Override
  public void add(T t) {
    delegateIterator.add(t);
  }
}

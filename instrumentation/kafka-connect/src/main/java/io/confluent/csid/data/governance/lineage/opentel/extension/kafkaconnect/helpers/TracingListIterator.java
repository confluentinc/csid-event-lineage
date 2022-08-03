/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import java.util.Iterator;
import java.util.ListIterator;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * Extends TracingIterator - Wraps ConnectRecord iterator and executes span creation and header
 * capture logic on next() / previous() calls.
 */
@Slf4j
public class TracingListIterator<T extends ConnectRecord<T>> extends TracingIterator<T>
    implements ListIterator<T> {

  private interface DelegateExcludes {

    <T extends ConnectRecord<T>> T previous();

    boolean hasPrevious();
  }

  @Delegate(excludes = {DelegateExcludes.class, Iterator.class})
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

}

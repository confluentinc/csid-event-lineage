/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import java.util.Collection;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectRecord;

@Slf4j
/**
 * Wrapper class providing iterator with tracing for ConnectRecord classes - used in instrumenting Connect sink / source tasks
 * as consumer records are first converted to internal connect records as a batch and only then
 * iterated - hence breaking span scope from initial consumer records iteration.
 */
public class TracingCollection<T extends ConnectRecord<T>> implements Collection<T> {

  private final Collection<T> delegate;
  protected final String spanName;

  public TracingCollection(Collection<T> delegate, String spanName) {
    this.spanName = spanName;
    this.delegate = delegate;
    log.trace("Creating TracingCollection spanName={}, delegate={}", spanName, delegate);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return delegate.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return new TracingIterator<>(delegate.iterator(), spanName);
  }

  @Override
  public Object[] toArray() {
    return delegate.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return delegate.toArray(a);
  }

  @Override
  public boolean add(T record) {
    return delegate.add(record);
  }

  @Override
  public boolean remove(Object o) {
    return delegate.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return delegate.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    return delegate.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return delegate.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return delegate.retainAll(c);
  }

  @Override
  public void clear() {
    delegate.clear();
  }
}

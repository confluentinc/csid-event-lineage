/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.UnaryOperator;
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

  private final List<T> delegate;

  /**
   * Wraps delegate List
   *
   * @param delegate list to wrap
   * @param spanName Span name to pass into iterator - for creating new spans on next() call.
   */
  public TracingList(List<T> delegate, String spanName) {
    super(delegate, spanName);
    this.delegate = delegate;
    log.trace("Creating TracingList spanName={}, delegate={}", spanName, delegate);

  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    return delegate.addAll(index, c);
  }

  @Override
  public void replaceAll(UnaryOperator<T> operator) {
    List.super.replaceAll(operator);
  }

  @Override
  public void sort(Comparator<? super T> c) {
    List.super.sort(c);
  }

  @Override
  public T get(int index) {
    return delegate.get(index);
  }

  @Override
  public T set(int index, T element) {
    return delegate.set(index, element);
  }

  @Override
  public void add(int index, T element) {
    delegate.add(index, element);
  }

  @Override
  public T remove(int index) {
    return delegate.remove(index);
  }

  @Override
  public int indexOf(Object o) {
    return delegate.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return delegate.lastIndexOf(o);
  }

  @Override
  public ListIterator<T> listIterator() {
    return new TracingListIterator<>(delegate.listIterator(), spanName);
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    return new TracingListIterator<>(delegate.listIterator(index), spanName);
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return new TracingList<>(delegate.subList(fromIndex, toIndex), spanName);
  }

  @Override
  public Spliterator<T> spliterator() {
    return List.super.spliterator();
  }
}

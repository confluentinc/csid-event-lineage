/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers;

import io.opentelemetry.javaagent.instrumentation.kafkaclients.TracingList;
import java.util.List;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Based on OpenTelemetry kafka-client {@link TracingList}.
 * <p>
 * Tracing is only implemented if using the List as Iterable but not for other List methods.
 * <p>
 * Will have to be revisited.
 */
public class HeaderCapturingList<K, V> extends HeaderCapturingIterable<K, V> implements
    List<ConsumerRecord<K, V>> {

  @Delegate(excludes = Iterable.class)
  private final List<ConsumerRecord<K, V>> delegate;

  public HeaderCapturingList(
      List<ConsumerRecord<K, V>> delegate) {
    super(delegate);
    this.delegate = delegate;
  }
}

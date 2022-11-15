/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import java.util.Iterator;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


public class TestConsumerInterceptor implements ConsumerInterceptor<String, String> {

  @Override
  public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
    //Cause trace generation by iterating.
    Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
    while (iterator.hasNext()) {
      iterator.next();
    }
    return records;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}

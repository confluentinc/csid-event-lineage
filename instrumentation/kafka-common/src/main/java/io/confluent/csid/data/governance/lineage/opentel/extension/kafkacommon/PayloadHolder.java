/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.Value;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Payload data holder for capturing payloads in Kafka Streams on Produce.
 * <p>
 * As Kafka Streams wraps {@link KafkaProducer} and performs serialization in advance - need to
 * capture payload early and hold on to it until actual {@link KafkaProducer#send} is invoked and
 * Producer Span is created.
 */
@Value
public class PayloadHolder {

  public static final ThreadLocal<PayloadHolder> PAYLOAD_HOLDER = new ThreadLocal<>();

  String key;
  String value;
}

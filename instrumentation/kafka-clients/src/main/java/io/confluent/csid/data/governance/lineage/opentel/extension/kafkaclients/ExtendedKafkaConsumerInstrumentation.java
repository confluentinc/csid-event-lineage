/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.returns;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ClusterIdHolder;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceMetadata;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceNameHolder;
import io.opentelemetry.instrumentation.api.field.VirtualField;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.time.Duration;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;

/**
 * Instrumentation advice for {@link KafkaConsumer#poll} method - captures Cluster ID from
 * ConsumerMetadata for later use.
 * <p>
 * Stores clusterId into {@link VirtualField} for extraction in
 * {@link ExtendedKafkaConsumerRecordIteratorInstrumentation} for Kafka Clients and Kafka Connect
 * modules.
 * <p>
 * Stores clusterId into ThreadLocal {@link ClusterIdHolder} for extraction in Kafka Streams
 * module.
 */
public class ExtendedKafkaConsumerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.clients.consumer.KafkaConsumer");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("poll")
            .and(isPublic())
            .and(takesArguments(1))
            .and(takesArgument(0, long.class).or(takesArgument(0, Duration.class)))
            .and(returns(named("org.apache.kafka.clients.consumer.ConsumerRecords"))),
        this.getClass().getName() + "$PollAdvice");
  }

  @SuppressWarnings("unused")
  public static class PollAdvice {

    @Advice.OnMethodExit()
    public static void onExit(
        @Advice.FieldValue("metadata") ConsumerMetadata metadata,
        @Advice.Return ConsumerRecords<?, ?> records) {

      // don't create spans when no records were received
      if (records == null || records.isEmpty()) {
        return;
      }
      VirtualField<ConsumerRecords<?, ?>, ServiceMetadata> metadataHolder =
          VirtualField.find(ConsumerRecords.class, ServiceMetadata.class);
      String clusterId = metadata.fetch().clusterResource().clusterId();
      metadataHolder.set(records,
          new ServiceMetadata(clusterId,
              ServiceNameHolder.get()));
      if (ClusterIdHolder.isEmpty()) {
        ClusterIdHolder.store(clusterId);
      }
    }
  }
}

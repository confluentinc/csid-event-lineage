/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.returns;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.HeaderCapturingIterable;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.HeaderCapturingIterator;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.HeaderCapturingList;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceMetadata;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceNameHolder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import io.opentelemetry.javaagent.instrumentation.kafkaclients.ConsumerRecordsInstrumentation;
import java.util.Iterator;
import java.util.List;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Based on OpenTelemetry kafka-clients {@link ConsumerRecordsInstrumentation}.
 * <p>
 * Applies Advice to {@link ConsumerRecords} class - wrapping returned Iterable, Iterator and List
 * to allow capturing of individual {@link ConsumerRecord} headers as they are traversed.
 */
public class ExtendedKafkaConsumerRecordIteratorInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.clients.consumer.ConsumerRecords");
  }

  /**
   * Defines methods to transform using Advice classes.
   * <p>
   * Note that Advice class names specified as String to avoid pre-mature class loading
   */
  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isMethod()
            .and(isPublic())
            .and(named("records"))
            .and(takesArgument(0, String.class))
            .and(returns(Iterable.class)),
        ExtendedKafkaConsumerRecordIteratorInstrumentation.class.getName() + "$IterableAdvice");
    transformer.applyAdviceToMethod(
        isMethod()
            .and(isPublic())
            .and(named("records"))
            .and(takesArgument(0, named("org.apache.kafka.common.TopicPartition")))
            .and(returns(List.class)),
        ExtendedKafkaConsumerRecordIteratorInstrumentation.class.getName() + "$ListAdvice");
    transformer.applyAdviceToMethod(
        isMethod()
            .and(isPublic())
            .and(named("iterator"))
            .and(takesArguments(0))
            .and(returns(Iterator.class)),
        ExtendedKafkaConsumerRecordIteratorInstrumentation.class.getName() + "$IteratorAdvice");
  }

  @SuppressWarnings("unused")
  public static class IterableAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static <K, V> void wrap(
        @Advice.This ConsumerRecords<K, V> consumerRecords,
        @Advice.Return(readOnly = false) Iterable<ConsumerRecord<K, V>> iterable) {
      if (iterable != null) {
        iterable = HeaderCapturingIterable.wrap(iterable,
            new ServiceMetadata(ServiceNameHolder.get()));
      }
    }
  }

  @SuppressWarnings("unused")
  public static class ListAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static <K, V> void wrap(
        @Advice.This ConsumerRecords<K, V> consumerRecords,
        @Advice.Return(readOnly = false) List<ConsumerRecord<K, V>> list) {
      if (list != null) {
        list = HeaderCapturingList.wrap(list,
            new ServiceMetadata(ServiceNameHolder.get()));
      }
    }
  }

  @SuppressWarnings("unused")
  public static class IteratorAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static <K, V> void wrap(
        @Advice.This ConsumerRecords<K, V> consumerRecords,
        @Advice.Return(readOnly = false) Iterator<ConsumerRecord<K, V>> iterator) {
      if (iterator != null) {
        iterator = HeaderCapturingIterator.wrap(iterator,
            new ServiceMetadata(ServiceNameHolder.get()));
      }
    }
  }
}

/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHolder.PAYLOAD_HOLDER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.payloadHandler;
import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHolder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.processor.internals.RecordCollector;

/**
 * RecordCollector instrumentation captures payload on KafkaStreams send before it is serialized.
 * Stores payload as Json string in ThreadLocal {@link PayloadHolder} on method exit clears
 * ThreadLocal.
 * <p>
 * Order of execution:
 * <p>
 * InstrumentationExtension.onEnter(...); - store payload into ThreadLocal
 * <p>
 * {@link RecordCollector#send} -- calls {@link KafkaProducer#send} - take payload from ThreadLocal
 * and capture Produce Span.
 * <p>
 * InstrumentationExtension.onExit(...); -- clear ThreadLocal
 */
public class KafkaRecordCollectorInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return hasSuperType(named("org.apache.kafka.streams.processor.internals.RecordCollector"));
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
            .and(named("send"))
            .and(takesArgument(4, Integer.class)),
        KafkaRecordCollectorInstrumentation.class.getName() + "$RecordCollectorSendAdvice");
  }

  @SuppressWarnings("unused")
  public static class RecordCollectorSendAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 1) Object key,
        @Advice.Argument(value = 2) Object value,
        @Advice.Local("payloadRecorded") boolean payloadRecorded) {

      if (PAYLOAD_HOLDER.get() == null && !(value instanceof byte[])) {
        payloadHandler().parseAndStorePayloadIntoPayloadHolder(key, value, PAYLOAD_HOLDER);
        payloadRecorded = true;
      }
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Advice.Local("payloadRecorded") boolean payloadRecorded) {
      if (payloadRecorded) {
        payloadRecorded = false;
        PAYLOAD_HOLDER.remove();
      }
    }
  }
}

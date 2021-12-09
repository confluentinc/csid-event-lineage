/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.payloadHandler;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHolder.PAYLOAD_HOLDER;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHolder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Kafka Producer instrumentation adds {@link KafkaProducer#send} advice that captures given {@link
 * ProducerRecord} key/value payloads and adds to current Span attributes.
 * <p>
 * Checks whether payload is held in ThreadLocal {@link PayloadHolder} and uses it instead - this
 * approach enables to capture and store payload before it is serialized to byte array in Kafka
 * Streams as {@link KafkaProducer} is used with {@link ByteArraySerializer} and object is
 * serialized in advance.
 */
public class ExtendedKafkaProducerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.clients.producer.KafkaProducer");
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
            .and(takesArgument(0, named("org.apache.kafka.clients.producer.ProducerRecord")))
            .and(takesArgument(1, named("org.apache.kafka.clients.producer.Callback"))),
        ExtendedKafkaProducerInstrumentation.class.getName()
            + "$ProducerPayloadCaptureAdvice");
  }

  @SuppressWarnings("unused")
  public static class ProducerPayloadCaptureAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0, readOnly = false) ProducerRecord<?, ?> producerRecord,
        @Advice.Argument(value = 1, readOnly = false) Callback callback) {

      Object key;
      Object value;
      PayloadHolder storedStreamsPayload = PAYLOAD_HOLDER.get();
      //Decide if stored payload from KStreams should be captured (if PayloadHolder thread local is set)
      //otherwise capture producerRecord payload.
      if (storedStreamsPayload != null) {
        key = storedStreamsPayload.getKey();
        value = storedStreamsPayload.getValue();
        PAYLOAD_HOLDER.remove();
      } else {
        key = producerRecord.key();
        value = producerRecord.value();
      }

      payloadHandler().captureKeyValuePayloadsToSpan(key, value,
          openTelemetryWrapper().currentSpan());
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void checkForError(@Advice.Thrown Throwable throwable) {
      if (throwable != null) {
        openTelemetryWrapper().currentSpan()
            .setAttribute(AttributeKey.stringKey("exception"), throwable.toString());
      }
    }
  }
}

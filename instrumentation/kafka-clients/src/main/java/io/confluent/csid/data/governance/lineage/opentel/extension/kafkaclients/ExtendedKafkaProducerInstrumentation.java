/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.headerCaptureConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.headersHandler;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.openTelemetryWrapper;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHolder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Kafka Producer instrumentation adds {@link KafkaProducer#send} advice that handles header
 * propagation and capture.
 * <p>
 * Checks whether headers are captured for propagation in the ThreadLocal holder and adds them to
 * produced record using configured whitelist
 * {@link HeaderCaptureConfiguration#getHeaderPropagationWhitelist()}
 * <p>
 * Captures set of headers into Span attributes using configured whitelist
 * {@link HeaderCaptureConfiguration#getHeaderCaptureWhitelist()}
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
            + "$ProducerExtensionAdvice");
  }

  @SuppressWarnings("unused")
  public static class ProducerExtensionAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0, readOnly = false) ProducerRecord<?, ?> producerRecord,
        @Advice.Argument(value = 1, readOnly = false) Callback callback) {

      //Skip all the header processing for changelog send operations
      if (producerRecord.topic().endsWith("-changelog")) {
        return;
      }
      //If there are stored headers from Consumer - set them on record (not overwriting any that already set)
      Header[] storedHeadersForPropagation = HeadersHolder.get().toArray();
      if (storedHeadersForPropagation.length > 0) {
        Header[] headersToPropagate = headersHandler().filterHeaders(storedHeadersForPropagation,
            headerCaptureConfiguration().getHeaderPropagationWhitelist());
        headersHandler().mergeHeaders(producerRecord.headers(),
            headersToPropagate);
      }
      //Capture headers configured for capture to span
      headersHandler().captureWhitelistedHeadersAsAttributesToCurrentSpan(
          producerRecord.headers().toArray());
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

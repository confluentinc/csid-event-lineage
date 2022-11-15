/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.Singletons.interceptorHandler;
import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.implementsInterface;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaclients.helpers.InterceptorHandler;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.SpanSuppressionConfiguration;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

/**
 * Applies Advice to classes implementing {@link ConsumerInterceptor} interface - to allow
 * suppression of tracing in Consumer Interceptors - based on configuration.
 * <p>
 * As Consumer process spans are generated through ConsumerRecords iterators - any interceptor
 * looping through records will cause additional process spans to be generated unnecessarily. This
 * Advice allows to disable such a behaviour.
 * <p>
 * Suppression of {@link ProducerInterceptors} is not required as the tracing is hooked to
 * Producer.send rather than record iterators.
 * </p>
 *
 * @see SpanSuppressionConfiguration#SPAN_SUPPRESSION_INTERCEPTOR_BLACKLIST_PROP
 * @see InterceptorHandler
 */
public class ConsumerInterceptorInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return implementsInterface(
        named("org.apache.kafka.clients.consumer.ConsumerInterceptor"));
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
            .and(named("onConsume")),
        ConsumerInterceptorInstrumentation.class.getName() + "$OnConsumeAdvice");

  }

  @SuppressWarnings("unused")
  public static class OnConsumeAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void setSuppression(@Advice.This ConsumerInterceptor<?, ?> interceptor) {
      if (interceptorHandler().interceptorShouldBeSuppressed(
          interceptor.getClass().getSimpleName())) {
        interceptorHandler().enableInterceptorSuppression();
      }
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void clearSuppression() {
      interceptorHandler().disableInterceptorSuppression();
    }
  }
}

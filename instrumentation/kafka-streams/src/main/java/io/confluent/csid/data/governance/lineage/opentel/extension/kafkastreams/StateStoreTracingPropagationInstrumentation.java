/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHolder.PAYLOAD_HOLDER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.STORED_TRACE_ID_HOLDER;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.stateStorePropagationHelpers;
import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.not;
import static net.bytebuddy.matcher.ElementMatchers.returns;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.PayloadHolder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.nio.charset.StandardCharsets;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.processor.StateStore;

/**
 * Handle tracing propagation for {@link StateStore} operations.
 * <p>
 * Add tracing information to byte[] serialized values as they are put into {@link StateStore}.
 * <p>
 * Read tracing information on get, strip it from byte[] serialized value and create a span.
 * <p>
 * Span creation behaviour depends on presence of active current span:
 * <p>
 * If active span is present - create new span with current active span as parent and a link to the
 * stored span that was extracted from retrieved value.
 * <p>
 * If active span is not present - create new span with extracted span as parent - as continuation
 * of extracted span.
 */
public class StateStoreTracingPropagationInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return hasSuperType(named("org.apache.kafka.streams.processor.StateStore"));
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
            .and(
                named("put")
                    .or(named("putIfAbsent")))
            .and(takesArgument(0, named("org.apache.kafka.common.utils.Bytes")))
            .and(takesArgument(1, byte[].class)),
        StateStoreTracingPropagationInstrumentation.class.getName() + "$StateStoreTracePutAdvice");
    transformer.applyAdviceToMethod(
        isMethod()
            .and(isPublic())
            .and(named("get"))
            .and(returns(byte[].class)),
        StateStoreTracingPropagationInstrumentation.class.getName()
            + "$StateStoreTraceGetBytesAdvice");
    transformer.applyAdviceToMethod(
        isMethod()
            .and(isPublic())
            .and(named("get"))
            .and(not(returns(byte[].class))),
        StateStoreTracingPropagationInstrumentation.class.getName()
            + "$StateStoreTraceGetObjectAdvice");
  }

  @SuppressWarnings("unused")
  public static class StateStoreTracePutAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(@Advice.Argument(value = 0) Object key,
        @Advice.Argument(value = 1, readOnly = false) byte[] value) {
      if (null == value) {
        return;
      }
      if (stateStorePropagationHelpers().hasTracingInfoAttached(value)) {
        return;
      }

      String traceIdentifier = openTelemetryWrapper()
          .traceIdStringFromContext(openTelemetryWrapper().currentContext());

      if (traceIdentifier != null) {
        value = stateStorePropagationHelpers().attachTracingInformation(value,
            traceIdentifier.getBytes(StandardCharsets.UTF_8));
      }

      // Should already have json of payload captured in the thread local
      // (as per StateStoreInstrumentation put advice) so just pull it out for span record.
      PayloadHolder storedStreamsPayload = PAYLOAD_HOLDER.get();
      if (storedStreamsPayload != null) {
        stateStorePropagationHelpers()
            .recordStateStorePutSpanWithPayload(traceIdentifier, storedStreamsPayload.getKey(),
                storedStreamsPayload.getValue());
      }
    }
  }

  public static class StateStoreTraceGetBytesAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.This Object thisObj,
        @Advice.Return(readOnly = false) byte[] bytesValue) {
      if (null == bytesValue) {
        return;
      }

      if (!stateStorePropagationHelpers().hasTracingInfoAttached(bytesValue)) {
        return;
      }

      Pair<String, byte[]> traceIdentifierAndStrippedValue = stateStorePropagationHelpers()
          .extractTracingInformation(bytesValue);

      String traceIdentifier = traceIdentifierAndStrippedValue.getLeft();
      bytesValue = traceIdentifierAndStrippedValue.getRight();

      STORED_TRACE_ID_HOLDER.set(traceIdentifier);
    }
  }


  public static class StateStoreTraceGetObjectAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Argument(value = 0) Object key,
        @Advice.Return Object rawValue) {
      if (null == rawValue) {
        return;
      }
      if (!(rawValue instanceof byte[])) {
        String storedTraceIdentifier = STORED_TRACE_ID_HOLDER.get();
        if (storedTraceIdentifier == null) {
          return;
        }
        STORED_TRACE_ID_HOLDER.remove();

        stateStorePropagationHelpers()
            .recordStateStoreGetSpanWithPayload(storedTraceIdentifier, key, rawValue);
      }
    }
  }
}

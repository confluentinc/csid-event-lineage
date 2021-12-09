/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.stateStorePropagationHelpers;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache;

/**
 * Helper instrumentation for handling tracing propagation for {@link StateStore} operations. Used
 * during put and fetch operations.
 * <p>
 * On Put - CachedStateStores are performing flush operation when cache size is exceeded - at that
 * point byte[] value is deserialized back into java object and trace header has to be stripped.
 * There is no need to capture it / record a span for it as it's already handled as part of the Put
 * operation that caused the cache size to be exceeded. See {@link ThreadCache#maybeEvict} and
 * {@link MeteredKeyValueStore#setFlushListener}
 * <p>
 * On Fetch - called during Stream to Stream join in {@link org.apache.kafka.streams.kstream.internals.KStreamKStreamJoin.KStreamKStreamJoinProcessor#process}
 * through {@link org.apache.kafka.streams.state.internals.MeteredWindowStoreIterator#next()}.
 * Retrieves stored trace id from value byte array and records StateStore Get span, strips the trace
 * id from value byte array for deserialization.
 */
public class StateSerdesTracingPropagationInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.state.StateSerdes");
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
                named("valueFrom"))
            .and(takesArgument(0, byte[].class)),
        StateSerdesTracingPropagationInstrumentation.class.getName()
            + "$StateSerdesValueFromAdvice");
  }

  public static class StateSerdesValueFromAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0, readOnly = false) byte[] bytesValue,
        @Advice.Local("storedTraceIdentifier") String storedTraceIdentifier) {
      if (null == bytesValue) {
        return;
      }
      if (!stateStorePropagationHelpers().hasTracingInfoAttached(bytesValue)) {
        return;
      }
      Pair<String, byte[]> traceIdentifierAndStrippedValue = stateStorePropagationHelpers()
          .extractTracingInformation(bytesValue);
      bytesValue = traceIdentifierAndStrippedValue.getRight();
      storedTraceIdentifier = traceIdentifierAndStrippedValue.getLeft();
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Advice.Local("storedTraceIdentifier") String storedTraceIdentifier,
        @Advice.Return Object value) {
      if (null == value) {
        return;
      }
      if (storedTraceIdentifier == null) {
        return;
      }
      Object key = Singletons.FETCH_KEY_HOLDER.get();
      if (key == null) {
        return;
      }

      stateStorePropagationHelpers()
          .recordStateStoreGetSpanWithPayload(storedTraceIdentifier, key, value);
    }
  }
}

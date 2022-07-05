/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.stateStorePropagationHelpers;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.TracingKeyValueStore;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder;

/**
 * Instrumentation for {@link KeyValueStoreBuilder} and {@link TimestampedKeyValueStoreBuilder}.
 * <p>
 * Intercepts KeyValueStore creation on return from {@link KeyValueStoreBuilder#maybeWrapCaching}
 * and wraps returned KeyValueStore with {@link TracingKeyValueStore}
 */
public class KeyValueStoreBuilderInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return
        named("org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder").or(
            named("org.apache.kafka.streams.state.internals.KeyValueStoreBuilder"));
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
            .and(isPrivate())
            .and(named("maybeWrapCaching")),
        KeyValueStoreBuilderInstrumentation.class.getName() + "$WrapStateStoreAdvice");
  }

  public static class WrapStateStoreAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Return(readOnly = false) KeyValueStore<Bytes, byte[]> stateStore) {
      stateStore = new TracingKeyValueStore(stateStorePropagationHelpers(),
          openTelemetryWrapper(),
          stateStore);
    }
  }
}

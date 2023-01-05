/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.stateStorePropagationHelpers;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.CACHE_LAYER;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.TracingSessionStore;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;

/**
 * Instrumentation for {@link SessionStoreBuilder}.
 * <p>
 * Intercepts SessionStore creation on return from {@link SessionStoreBuilder#maybeWrapCaching} and
 * {@link SessionStoreBuilder#maybeWrapLogging} and wraps returned SessionStore with
 * {@link TracingSessionStore}
 */
public class SessionStoreBuilderInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return
        named("org.apache.kafka.streams.state.internals.SessionStoreBuilder");
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
            .and(named("maybeWrapLogging")),
        SessionStoreBuilderInstrumentation.class.getName() + "$WrapStateStoreAdvice");
    transformer.applyAdviceToMethod(
        isMethod()
            .and(isPrivate())
            .and(named("maybeWrapCaching")),
        SessionStoreBuilderInstrumentation.class.getName() + "$WrapCachingStateStoreAdvice");
  }

  public static class WrapStateStoreAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Return(readOnly = false) SessionStore<Bytes, byte[]> stateStore) {
      stateStore = new TracingSessionStore(stateStorePropagationHelpers(),
          openTelemetryWrapper(),
          stateStore,
          CACHE_LAYER.NO);
    }
  }

  public static class WrapCachingStateStoreAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Return(readOnly = false) SessionStore<Bytes, byte[]> stateStore) {
      if (!(stateStore instanceof TracingSessionStore)) {
        stateStore = new TracingSessionStore(stateStorePropagationHelpers(),
            openTelemetryWrapper(),
            stateStore,
            CACHE_LAYER.YES);
      }
    }
  }
}

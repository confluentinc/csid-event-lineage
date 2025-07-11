/*
 * Copyright 2022 Confluent Inc.
 */
package org.apache.kafka.streams.state.internals;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.openTelemetryWrapper;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isPackagePrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.slf4j.event.Level.TRACE;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.CacheHandlerFlag;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.LoggerBridge;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.field.VirtualField;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Instrumentation for {@link LRUCacheEntry}.
 * <p>
 * Contains two advices
 * <p>
 * ConstructorAdvice - intercepts constructor and records association between this entry and current
 * trace context in in-memory cache.
 * <p>
 * HashCodeAdvice - overrides hashCode to return System.identityHashCode to enable using the object
 * as a key in the in-memory cache.
 */
public class LRUCacheEntryInstrumentation implements
    TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return
        named(
            "org.apache.kafka.streams.state.internals.LRUCacheEntry");
  }

  /**
   * Defines methods to transform using Advice classes.
   * <p>
   * Note that Advice class names specified as String to avoid pre-mature class loading
   */
  @Override
  public void transform(TypeTransformer transformer) {

    transformer.applyAdviceToMethod(
        isConstructor()
            .and(isPackagePrivate()),
        LRUCacheEntryInstrumentation.class.getName()
            + "$ConstructorAdvice");
    transformer.applyAdviceToMethod(named("hashCode"),
        LRUCacheEntryInstrumentation.class.getName()
            + "$HashCodeAdvice");
  }

  public static class ConstructorAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.This LRUCacheEntry lruCacheEntry) {
      if (CacheHandlerFlag.isEnabled()) {
        VirtualField<LRUCacheEntry, Context> contextField = VirtualField.find(LRUCacheEntry.class,
            Context.class);
        contextField.set(lruCacheEntry, openTelemetryWrapper().currentContext());
        LoggerBridge.log(TRACE,
            "Set Tracing context to cache for recordContext {}, offset {}, traceContext {} ",
            lruCacheEntry.context().toString(), lruCacheEntry.context().offset(),
            openTelemetryWrapper().currentContext());
      }
    }
  }

  public static class HashCodeAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Return(readOnly = false) Integer hashCode,
        @Advice.This LRUCacheEntry lruCacheEntry) {
      if (CacheHandlerFlag.isEnabled()) {
        hashCode = System.identityHashCode(lruCacheEntry);
      }
    }
  }
}

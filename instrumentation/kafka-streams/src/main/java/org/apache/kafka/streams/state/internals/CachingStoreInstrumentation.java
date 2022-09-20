/*
 * Copyright 2022 Confluent Inc.
 */
package org.apache.kafka.streams.state.internals;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.spanHandler;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.CacheHandlerFlag;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.field.VirtualField;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.Argument;
import net.bytebuddy.asm.Advice.This;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

public class CachingStoreInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.state.internals.CachingKeyValueStore").or(
        named("org.apache.kafka.streams.state.internals.CachingWindowStore").or(
            named("org.apache.kafka.streams.state.internals.CachingSessionStore")));
  }

  /**
   * Defines methods to transform using Advice classes.
   * <p>
   * Note that Advice class names specified as String to avoid pre-mature class loading
   */
  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("putAndMaybeForward")
            .and(takesArguments(2)),
        CachingStoreInstrumentation.class.getName()
            + "$PutAndMaybeForwardAdvice");
    transformer.applyAdviceToMethod(
        named("putInternal")
            .and(takesArguments(2)),
        CachingStoreInstrumentation.class.getName()
            + "$PutInternalAdvice");

  }

  public static class PutAndMaybeForwardAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Argument(value = 0, readOnly = false) ThreadCache.DirtyEntry entry,
        @This CachingKeyValueStore cachingKeyValueStore,
        @Advice.Local("scope") Scope scope,
        @Advice.Local("flushSpan") Span flushSpan) {
      CacheHandlerFlag.enable();
      VirtualField<LRUCacheEntry, Context> cacheTraceContextStore = VirtualField.find(
          LRUCacheEntry.class, Context.class);
      Context storedContext = cacheTraceContextStore.get(
          entry.entry());
      System.out.println(
          "Got context from cache for key " + entry.key() + ", record " + entry.entry().context()
              .toString() +
              ", offset " + entry.entry().context().offset()
              + ", context " + (storedContext != null ? storedContext.toString() : "null"));
      if (storedContext != null) {
        flushSpan = spanHandler().createAndStartSpan(
            String.format(SpanNames.STATE_STORE_SPAN_NAME_FORMAT,
                cachingKeyValueStore.name() + "-cache", SpanNames.STATE_STORE_FLUSH),
            storedContext);
        scope = flushSpan.makeCurrent();
      }
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void onExit(
        @Advice.Local("scope") Scope scope,
        @Advice.Local("flushSpan") Span flushSpan) {
      CacheHandlerFlag.disable();
      if (flushSpan != null) {
        flushSpan.end();
        scope.close();
      }
    }
  }

  public static class PutInternalAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter() {
      CacheHandlerFlag.enable();
    }

    @Advice.OnMethodExit()
    public static void onExit() {
      CacheHandlerFlag.disable();
    }
  }
}

/*
 * Copyright 2022 Confluent Inc.
 */
package org.apache.kafka.streams.state.internals;

import static net.bytebuddy.matcher.ElementMatchers.named;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.CacheHandlerFlag;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

public class KeyValueCachingStoreInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.state.internals.CachingKeyValueStore");
  }

  /**
   * Defines methods to transform using Advice classes.
   * <p>
   * Note that Advice class names specified as String to avoid pre-mature class loading
   */
  @Override
  public void transform(TypeTransformer transformer) {

    transformer.applyAdviceToMethod(
        named("putInternal")
           .or(named("deleteInternal")),
        KeyValueCachingStoreInstrumentation.class.getName()
            + "$EnableCacheHandlerAdvice");

  }

  public static class EnableCacheHandlerAdvice {

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

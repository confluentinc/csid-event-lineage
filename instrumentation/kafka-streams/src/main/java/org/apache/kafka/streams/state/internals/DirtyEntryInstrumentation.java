/*
 * Copyright 2022 Confluent Inc.
 */
package org.apache.kafka.streams.state.internals;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.stateStorePropagationHelpers;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.CacheHandlerFlag;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Instrumentation for {@link ThreadCache.DirtyEntry}.
 * <p>
 * Strips tracing data and restores original payload value on access to newValue field.
 */
public class DirtyEntryInstrumentation implements
    TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return
        named(
            "org.apache.kafka.streams.state.internals.ThreadCache$DirtyEntry");
  }

  /**
   * Defines methods to transform using Advice classes.
   * <p>
   * Note that Advice class names specified as String to avoid pre-mature class loading
   */
  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(named("newValue"),
        DirtyEntryInstrumentation.class.getName()
            + "$NewValueAdvice");
  }

  public static class NewValueAdvice {
    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Return(readOnly = false) byte[] value) {
      if (CacheHandlerFlag.isEnabled()) {
        value = stateStorePropagationHelpers().restoreRawValue(value);
      }
    }
  }
}

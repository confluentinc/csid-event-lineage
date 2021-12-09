/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.FETCH_KEY_HOLDER;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Handles payload capture during Stream to Stream join - see {@link
 * org.apache.kafka.streams.kstream.internals.KStreamKStreamJoin.KStreamKStreamJoinProcessor#process}
 * Stores message key into thread local for capturing and to indicate that during {@link
 * org.apache.kafka.streams.state.StateSerdes#valueFrom(byte[])} {@link
 * StateSerdesTracingPropagationInstrumentation} should record trace.
 */
public class KStreamKStreamJoinInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named(
        "org.apache.kafka.streams.kstream.internals.KStreamKStreamJoin$KStreamKStreamJoinProcessor");
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
            .and(named("process")),
        KStreamKStreamJoinInstrumentation.class.getName() + "$ProcessAdvice");
  }

  @SuppressWarnings("unused")
  public static class ProcessAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0) Object key,
        @Advice.Argument(value = 1) Object value) {
      FETCH_KEY_HOLDER.set(key);
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit() {
      FETCH_KEY_HOLDER.remove();
    }
  }
}

/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHolder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

public class StreamTaskInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.processor.internals.StreamTask");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("process").and(isPublic()),
        StreamTaskInstrumentation.class.getName() + "$ProcessAdvice");
  }

  /**
   * The method decorated by this advice calls PartitionGroup.nextRecord(), which triggers
   * PartitionGroupInstrumentation that actually starts the span (in opentelemetry agent) and
   * another one (in this extension) that captures headers into thread local {@link HeadersHolder}
   * <p>
   * This OnExit advice clears the headers in the thread local {@link HeadersHolder} as event is now
   * processed.
   */
  @SuppressWarnings("unused")
  public static class ProcessAdvice {

    @Advice.OnMethodExit
    public static void stopSpan() {
      HeadersHolder.clear();
    }
  }
}

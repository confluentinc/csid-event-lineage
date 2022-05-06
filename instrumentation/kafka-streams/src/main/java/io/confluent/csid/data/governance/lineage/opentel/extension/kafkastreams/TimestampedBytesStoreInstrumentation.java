/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.valueAndTimestampHandler;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.Return;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.streams.state.TimestampedBytesStore;

/**
 * Instrumentation for {@link TimestampedBytesStore#convertToTimestampedFormat}.
 * <p>
 * Injects logic performing Timestamp+Trace+Value rearrangement into Trace+Timestamp+Value order
 */
public class TimestampedBytesStoreInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.state.TimestampedBytesStore");
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
            .and(
                named("convertToTimestampedFormat"))
            .and(takesArgument(0, byte[].class)),
        TimestampedBytesStoreInstrumentation.class.getName()
            + "$ConvertToTimestampedFormatAdvice");

  }

  public static class ConvertToTimestampedFormatAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Return(readOnly = false) byte[] toReturn) {
      if (toReturn == null) {
        return;
      }

      byte[] rearranged = valueAndTimestampHandler().rearrangeTimestampAndValue(toReturn);
      toReturn = rearranged;
    }
  }
}

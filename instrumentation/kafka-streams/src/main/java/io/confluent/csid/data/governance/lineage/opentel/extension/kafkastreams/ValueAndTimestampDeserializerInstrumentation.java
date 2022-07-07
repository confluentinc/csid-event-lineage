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
import net.bytebuddy.asm.Advice.Local;
import net.bytebuddy.asm.Advice.Return;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Instrumentation for
 * {@link org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer}.
 * <p>
 * Injects logic for rawValue and rawTimestamp extraction from Trace+Timestamp+Value composite
 * payloads.
 */
public class ValueAndTimestampDeserializerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer");
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
                named("rawValue"))
            .and(takesArgument(0, byte[].class)),
        ValueAndTimestampDeserializerInstrumentation.class.getName()
            + "$RawValueAdvice");
    transformer.applyAdviceToMethod(
        isMethod()
            .and(
                named("rawTimestamp"))
            .and(takesArgument(0, byte[].class)),
        ValueAndTimestampDeserializerInstrumentation.class.getName()
            + "$RawTimestampAdvice");
  }

  public static class RawValueAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0, readOnly = false) byte[] bytesValue,
        @Local("valueWithoutTimestamp") byte[] valueWithoutTimestamp) {
      valueWithoutTimestamp = valueAndTimestampHandler().rawValue(bytesValue);
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Argument(value = 0, readOnly = false) byte[] bytesValue,
        @Local("valueWithoutTimestamp") byte[] valueWithoutTimestamp,
        @Return(readOnly = false) byte[] toReturn) {
      if (null == bytesValue || valueWithoutTimestamp == null) {
        return;
      }
      toReturn = valueWithoutTimestamp;
    }
  }

  public static class RawTimestampAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0, readOnly = false) byte[] bytesValue) {
      bytesValue = valueAndTimestampHandler().rawTimestamp(bytesValue);
    }
  }
}

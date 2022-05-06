/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.valueAndTimestampHandler;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isStatic;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.Return;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.apache.kafka.streams.state.internals.RecordConverters;

/**
 * Instruments {@link RecordConverters#rawValueToTimestampedValue}.
 * <p>
 * Injects logic performing Timestamp+Trace+Value rearrangement into Trace+Timestamp+Value order.
 * Used during state store restoration.
 */
public class RecordConvertersInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.state.internals.RecordConverters");
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
            .and(named("rawValueToTimestampedValue"))
            .and(isStatic()),
        RecordConvertersInstrumentation.class.getName()
            + "$ReturnCompositeRearrangingConverterAdvice");

  }

  public static class ReturnCompositeRearrangingConverterAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Return(readOnly = false) RecordConverter toReturn) {
      toReturn = valueAndTimestampHandler().rearrangingRawValueToTimestampedValueConverter(
          toReturn);
    }
  }
}

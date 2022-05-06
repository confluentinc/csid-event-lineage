/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.headersHandler;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPackagePrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.returns;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeadersHolder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.streams.processor.internals.PartitionGroup;
import org.apache.kafka.streams.processor.internals.StampedRecord;

/**
 * Based on OpenTelemetry kafka-streams PartitionGroupInstrumentation.
 * <p>
 * Store configured headers in {@link HeadersHolder}  for automatic propagation on produce.
 * <p>
 * Capture header key/values as Span attributes according to configured whitelist. Headers are
 * captured according to configured  as is (as byte[] values) and recorded to consume span assuming
 * String values.
 * <p>
 * {@link PartitionGroup#nextRecord} advice in OpenTelemetry kafka-streams instrumentation starts
 * the span to which captured header attributes are added.
 */
public class PartitionGroupInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.streams.processor.internals.PartitionGroup");
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
            .and(isPackagePrivate())
            .and(named("nextRecord"))
            .and(returns(named("org.apache.kafka.streams.processor.internals.StampedRecord"))),
        PartitionGroupInstrumentation.class.getName() + "$NextRecordAdvice");
  }

  @SuppressWarnings("unused")
  public static class NextRecordAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Advice.Return StampedRecord record) {
      if (record == null) {
        return;
      }
      headersHandler().captureWhitelistedHeadersAsAttributesToCurrentSpan(
          record.headers().toArray());
    }
  }
}

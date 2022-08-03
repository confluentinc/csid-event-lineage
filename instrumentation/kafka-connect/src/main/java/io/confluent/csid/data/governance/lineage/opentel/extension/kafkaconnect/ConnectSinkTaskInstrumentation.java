/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperClass;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.TracingCollection;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.Collection;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * Kafka Connect Sink Task instrumentation wraps ConnectRecord collection with
 * {@link TracingCollection} on {@link SinkTask#put(Collection)} method to inject tracing capability
 * for SinkTask execution.
 * <p>
 * As the ConnectRecord collection is traversed during task execution - tracing logic in
 * TracingIterator is invoked and "sinkTask-process" Spans are recorded and headers captured.
 */
public class ConnectSinkTaskInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return hasSuperClass(named("org.apache.kafka.connect.sink.SinkTask"));
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
            .and(named("put"))
            .and(takesArguments(1)),
        ConnectSinkTaskInstrumentation.class.getName()
            + "$SinkTaskPutAdvice");
  }

  @SuppressWarnings("unused")
  public static class SinkTaskPutAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0, readOnly = false) Collection<SinkRecord> sinkRecords) {
      sinkRecords = new TracingCollection<>(sinkRecords, SpanNames.SINK_TASK);
    }
  }
}

/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.openTelemetryWrapper;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.field.VirtualField;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.Collection;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * Kafka Connect Worker Sink Task instrumentation captures current Consumer Process span correlated
 * with transformed SinkRecord for later use as parent during iteration over SinkRecords in
 * {@link SinkTask#put(Collection)} execution.
 */
public class ConnectWorkerSinkTaskInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.connect.runtime.WorkerSinkTask");
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
            .and(isPrivate())
            .and(named("convertAndTransformRecord"))
            .and(takesArguments(1)),
        ConnectWorkerSinkTaskInstrumentation.class.getName()
            + "$WorkerSinkTaskConvertAndTransformRecordAdvice");
  }

  @SuppressWarnings("unused")
  public static class WorkerSinkTaskConvertAndTransformRecordAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Advice.Return SinkRecord sinkRecord) {
      //method is executed in consumer process context - store it associated with converted record
      //for use as parent context in downstream SinkTask.put when iterating over SinkRecords.
      VirtualField<SinkRecord, Context> sinkRecordContextStore = VirtualField.find(
          SinkRecord.class, Context.class);
      sinkRecordContextStore.set(sinkRecord, openTelemetryWrapper().currentContext());
    }
  }
}

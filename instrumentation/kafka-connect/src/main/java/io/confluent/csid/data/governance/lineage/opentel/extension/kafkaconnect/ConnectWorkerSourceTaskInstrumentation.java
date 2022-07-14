/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isProtected;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.SourcePollMarkerHolder;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.TracingCollection;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.TracingList;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.List;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Kafka Connect Source Task instrumentation wraps ConnectRecord collection with
 * {@link TracingCollection} on return of
 * {@link org.apache.kafka.connect.runtime.WorkerSourceTask#poll()} method to inject tracing
 * capability for SourceTask execution.
 * <p>
 * As the ConnectRecord collection is traversed during task execution - tracing logic in
 * TracingIterator is invoked and "sourceTask" Spans are recorded and headers captured.
 */
public class ConnectWorkerSourceTaskInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.connect.runtime.WorkerSourceTask");
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
            .and(isProtected())
            .and(named("poll"))
            .and(takesArguments(0)),
        ConnectWorkerSourceTaskInstrumentation.class.getName()
            + "$WorkerSourceTaskPollAdvice");
  }

  @SuppressWarnings("unused")
  public static class WorkerSourceTaskPollAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter() {
      //set marker to enable context capture in SourceRecord constructor
      SourcePollMarkerHolder.enableTracing();
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(
        @Advice.Return(readOnly = false) List<SourceRecord> sourceRecords) {
      SourcePollMarkerHolder.disableTracing();
      if (sourceRecords != null && !sourceRecords.isEmpty()) {
        sourceRecords = new TracingList<>(sourceRecords, SpanNames.SOURCE_TASK);
      }
    }
  }
}

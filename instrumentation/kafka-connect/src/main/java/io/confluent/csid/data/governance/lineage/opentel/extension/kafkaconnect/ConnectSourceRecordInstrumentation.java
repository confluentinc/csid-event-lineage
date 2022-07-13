package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.openTelemetryWrapper;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.SourcePollMarkerHolder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.field.VirtualField;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.connect.source.SourceRecord;

public class ConnectSourceRecordInstrumentation implements
    TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.kafka.connect.source.SourceRecord");
  }

  /**
   * Defines methods to transform using Advice classes.
   * <p>
   * Note that Advice class names specified as String to avoid pre-mature class loading
   */
  @Override
  public void transform(TypeTransformer transformer) {

    transformer.applyAdviceToMethod(
        isConstructor().and(isPublic()).and(takesArguments(10)),
        ConnectSourceRecordInstrumentation.class.getName()
            + "$SourceRecordConstructorAdvice");
  }

  public static class SourceRecordConstructorAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Advice.This SourceRecord sourceRecord) {
      //capture current tracing context and associate to SourceRecord created.
      //Only execute if inside SourceTask.poll call - indicated by SourcePollMarker.
      if (SourcePollMarkerHolder.get()) {
        VirtualField<SourceRecord, Context> sourceRecordContextStore = VirtualField.find(
            SourceRecord.class, Context.class);
        sourceRecordContextStore.set(sourceRecord, openTelemetryWrapper().currentContext());
      }
    }
  }
}

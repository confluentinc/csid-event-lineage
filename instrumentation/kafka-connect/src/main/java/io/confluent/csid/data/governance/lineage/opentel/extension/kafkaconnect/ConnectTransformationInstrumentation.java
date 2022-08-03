/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.connectHandler;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.headerCaptureConfiguration;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.helpers.Singletons.spanHandler;
import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.Constants.SpanNames;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.HeaderCaptureConfiguration;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Kafka Connect Transformation instrumentation adds {@link Transformation#apply(ConnectRecord)}
 * advice.
 * <p>
 * Creates a Span wrapping the Transformation operation and captures headers as configured by
 * {@link HeaderCaptureConfiguration}
 */
public class ConnectTransformationInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return hasSuperType(named("org.apache.kafka.connect.transforms.Transformation"));
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
            .and(named("apply"))
            .and(takesArguments(1)),
        ConnectTransformationInstrumentation.class.getName()
            + "$TransformationApplyAdvice");
  }

  @SuppressWarnings("unused")
  public static class TransformationApplyAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0) ConnectRecord connectRecord,
        @Advice.This Transformation transformation,
        @Advice.Local("scope") Scope scope,
        @Advice.Local("smtSpan") Span smtSpan) {
      //Create and start SMT span
      //Pass scope / span object to onExit through local - to close it on transformation completion.
      smtSpan = spanHandler().createAndStartSpan(
          String.format(SpanNames.SMT_SPAN_NAME_FORMAT, SpanNames.SMT,
              transformation.getClass().getSimpleName()),
          openTelemetryWrapper().currentContext());
      scope = smtSpan.makeCurrent();
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void onExit(@Advice.Thrown Throwable throwable,
        @Advice.Return() ConnectRecord transformedRecord,
        @Advice.Local("scope") Scope scope,
        @Advice.Local("smtSpan") Span smtSpan) {
      if (throwable != null) {
        smtSpan.setAttribute(AttributeKey.stringKey("exception"), throwable.toString());
      } else {
        //Capture headers in onExit - postSMT (as whitelisted for capture)
        connectHandler().captureConnectHeadersToCurrentSpan(transformedRecord.headers(),
            headerCaptureConfiguration().getHeaderValueEncoding());
      }
      smtSpan.end();
      scope.close();
    }
  }
}

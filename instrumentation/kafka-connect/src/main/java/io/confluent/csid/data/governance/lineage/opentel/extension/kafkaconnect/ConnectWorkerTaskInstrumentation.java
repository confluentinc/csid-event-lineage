/*
 * Copyright 2021 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperClass;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.ServiceNameHolder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.connect.util.ConnectorTaskId;

;

/**
 * Instrumentation for sub-classes of {@link org.apache.kafka.connect.runtime.WorkerTask} class -
 * advice for {@link org.apache.kafka.connect.runtime.WorkerTask#execute()} method capturing
 * Connector name for storing into ThreadLocal {@link ServiceNameHolder} and for subsequent service
 * name overriding.
 */
public class ConnectWorkerTaskInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return hasSuperClass(named("org.apache.kafka.connect.runtime.WorkerTask"));
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
            .and(named("execute"))
            .and(takesArguments(0)),
        ConnectWorkerTaskInstrumentation.class.getName()
            + "$ExecuteAdvice");
  }

  @SuppressWarnings("unused")
  public static class ExecuteAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.FieldValue(value = "id") ConnectorTaskId connectorTaskId) {
      ServiceNameHolder.store(connectorTaskId.connector());
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit() {
      ServiceNameHolder.clear();
    }
  }
}


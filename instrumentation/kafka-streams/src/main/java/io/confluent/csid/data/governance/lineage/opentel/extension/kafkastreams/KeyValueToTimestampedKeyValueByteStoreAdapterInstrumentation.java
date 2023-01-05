/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams;

import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.openTelemetryWrapper;
import static io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.Singletons.stateStorePropagationHelpers;
import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isPackagePrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon.CACHE_LAYER;
import io.confluent.csid.data.governance.lineage.opentel.extension.kafkastreams.helpers.TracingKeyValueStore;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueToTimestampedKeyValueByteStoreAdapter;

/**
 * Instrumentation for {@link KeyValueToTimestampedKeyValueByteStoreAdapter}.
 * <p>
 * Intercepts constructor and wraps passed in KeyValueStore with {@link TracingKeyValueStore}.
 */
public class KeyValueToTimestampedKeyValueByteStoreAdapterInstrumentation implements
    TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return
        named(
            "org.apache.kafka.streams.state.internals.KeyValueToTimestampedKeyValueByteStoreAdapter");
  }

  /**
   * Defines methods to transform using Advice classes.
   * <p>
   * Note that Advice class names specified as String to avoid pre-mature class loading
   */
  @Override
  public void transform(TypeTransformer transformer) {

    transformer.applyAdviceToMethod(
        isConstructor()
            .and(isPackagePrivate()),
        KeyValueToTimestampedKeyValueByteStoreAdapterInstrumentation.class.getName()
            + "$ConstructorAdvice");
  }

  public static class ConstructorAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.Argument(value = 0, readOnly = false) KeyValueStore<Bytes, byte[]> store) {
      store = new TracingKeyValueStore(stateStorePropagationHelpers(), openTelemetryWrapper(),
          store, CACHE_LAYER.NO);
    }
  }
}

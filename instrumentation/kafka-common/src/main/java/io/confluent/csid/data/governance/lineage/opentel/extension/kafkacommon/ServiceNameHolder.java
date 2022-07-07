/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.Value;

/**
 * Service name holder used to enable service name overriding in multi service applications (for
 * example Kafka Connect)
 * <p>
 * Service name stored into holder at high level service / thread bound operation and extracted
 * later appropriate span processing stage - for example in consumer record iteration. Suggested use
 * with context propagation customizer to set service name once per record flow - set on first local
 * span creation and propagate through all the local child spans.
 */
@Value
public class ServiceNameHolder {

  private static final ThreadLocal<String> SERVICENAME_HOLDER = new ThreadLocal<>();

  public static void store(String serviceName) {
    SERVICENAME_HOLDER.set(serviceName);
  }

  public static String get() {
    return SERVICENAME_HOLDER.get();
  }

  public static void clear() {
    SERVICENAME_HOLDER.remove();
  }
}

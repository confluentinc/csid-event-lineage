/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

/**
 * ThreadLocal for passing ClusterId from consumer to KStream instrumentation. Since in KStreams
 * cross-cluster operations are not supported - can just set once on first consumer poll and leave
 * as that.
 */
public class ClusterIdHolder {

  private static final ThreadLocal<String> CLUSTER_ID_HOLDER = new ThreadLocal<>();

  public static void store(String clusterId) {
    CLUSTER_ID_HOLDER.set(clusterId);
  }

  public static String get() {
    return CLUSTER_ID_HOLDER.get();
  }

  public static void clear() {
    CLUSTER_ID_HOLDER.remove();
  }

  public static boolean isEmpty() {
    return CLUSTER_ID_HOLDER.get() == null || CLUSTER_ID_HOLDER.get().length() == 0;
  }
}

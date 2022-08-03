/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;

/**
 * <p>
 * Adapted for use in integration tests from Apache Kafka Connect Standalone CLI starter utility.
 * <p>
 * Runs Kafka Connect as a standalone process. In this mode, work is not distributed. Instead, all
 * the normal Connect machinery works within a single process. This is useful for ad hoc, small, or
 * experimental jobs.
 * </p>
 * <p>
 * By default, no job configs or offset data is persistent. You can make jobs persistent and fault
 * tolerant by overriding the settings to use file storage for both.
 * </p>
 */
@Slf4j
@RequiredArgsConstructor
public class ConnectStandalone {

  private Connect connect;
  private final Properties workerProperties;
  private final Properties connectorProperties;

  public void start() {

    try {
      Time time = Time.SYSTEM;
      log.info("Kafka Connect standalone worker initializing ...");
      long initStart = time.hiResClockMs();
      WorkerInfo initInfo = new WorkerInfo();
      initInfo.logAll();

      Map<String, String> workerProps = Utils.propsToStringMap(workerProperties);
      log.info("Scanning for plugin classes. This might take a moment ...");
      Plugins plugins = new Plugins(workerProps);
      plugins.compareAndSwapWithDelegatingLoader();
      StandaloneConfig config = new StandaloneConfig(workerProps);

      String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
      log.debug("Kafka cluster ID: {}", kafkaClusterId);

      RestServer rest = new RestServer(config);
      rest.initializeServer();

      URI advertisedUrl = rest.advertisedUrl();
      String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

      ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
          config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
          config, ConnectorClientConfigOverridePolicy.class);
      Worker worker = new Worker(workerId, time, plugins, config, new FileOffsetBackingStore(),
          connectorClientConfigOverridePolicy);

      Herder herder = new StandaloneHerder(worker, kafkaClusterId,
          connectorClientConfigOverridePolicy);
      connect = new Connect(herder, rest);
      log.info("Kafka Connect standalone worker initialization took {}ms",
          time.hiResClockMs() - initStart);

      try {
        connect.start();

        Map<String, String> connectorProps = Utils.propsToStringMap(connectorProperties);
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(
            new Callback<Herder.Created<ConnectorInfo>>() {
              @Override
              public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                if (error != null) {
                  log.error("Failed to create job for {}", connectorProperties);
                } else {
                  log.info("Created connector {}", info.result().name());
                }
              }
            });
        herder.putConnectorConfig(
            connectorProps.get(ConnectorConfig.NAME_CONFIG),
            connectorProps, false, cb);
        cb.get();

      } catch (Throwable t) {
        log.error("Stopping after connector error", t);
        connect.stop();
      }

    } catch (Throwable t) {
      log.error("Stopping due to error", t);
      try {
        if (connect != null && connect.isRunning()) {
          connect.stop();
        }
      } catch (Exception e) {
      } // suppressed
    }
  }

  public void stop() {
    try {
      if (connect != null && connect.isRunning()) {
        connect.stop();
      }
    } catch (Exception e) {
    } // suppressed
  }
}


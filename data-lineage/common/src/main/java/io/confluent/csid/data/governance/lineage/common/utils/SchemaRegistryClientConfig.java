package io.confluent.csid.data.governance.lineage.common.utils;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Schema registry configuration
 */
public class SchemaRegistryClientConfig extends AbstractKafkaSchemaSerDeConfig {

    private static final ConfigDef config;

    static {
        config = baseConfigDef();
    }

    public SchemaRegistryClientConfig(Map<?, ?> props) {
        super(config, props);
    }
}
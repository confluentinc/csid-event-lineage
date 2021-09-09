package io.confluent.csid.data.governance.lineage.common.utils;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.common.config.ConfigException;

import java.util.*;

public class SchemaRegistryUtils {
    private static final String MOCK_URL_PREFIX = "mock://";

    public static SchemaRegistryClient getSchemaRegistryClient(Map<?, ?> props) {
        final SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(props);

        List<String> urls = config.getSchemaRegistryUrls();
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        Map<String, Object> originals = config.originalsWithPrefix("");

        final SchemaRegistryClient schemaRegistry;

        String mockScope = validateAndMaybeGetMockScope(urls);
        List<SchemaProvider> providers = Collections.singletonList(
                new AvroSchemaProvider());

        if (mockScope != null) {
            schemaRegistry = MockSchemaRegistry.getClientForScope(mockScope, providers);
        } else {
            schemaRegistry = new CachedSchemaRegistryClient(
                    urls,
                    maxSchemaObject,
                    providers,
                    originals,
                    config.requestHeaders()
            );
        }

        return schemaRegistry;
    }

    private static String validateAndMaybeGetMockScope(final List<String> urls) {
        final List<String> mockScopes = new LinkedList<>();
        for (final String url : urls) {
            if (url.startsWith(MOCK_URL_PREFIX)) {
                mockScopes.add(url.substring(MOCK_URL_PREFIX.length()));
            }
        }

        if (mockScopes.isEmpty()) {
            return null;
        } else if (mockScopes.size() > 1) {
            throw new ConfigException(
                    "Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls
            );
        } else if (urls.size() > mockScopes.size()) {
            throw new ConfigException(
                    "Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls
            );
        } else {
            return mockScopes.get(0);
        }
    }
}

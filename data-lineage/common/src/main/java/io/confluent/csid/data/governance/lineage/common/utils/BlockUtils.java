package io.confluent.csid.data.governance.lineage.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class BlockUtils {
    private final static Logger LOGGER = LoggerFactory.getLogger(BlockUtils.class);

    private static MessageDigest messageDigest;
    public static int prefix = 4;
    public static String prefixString = new String(new char[prefix]).replace('\0', '0');

    static {
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Error getting message digest.", e);
        }
    }

    public static String getDataHash(final byte[] data) throws NoSuchAlgorithmException {
        return Base64
                .getEncoder()
                .encodeToString(messageDigest.digest(data));
    }

    public static byte[] getDigest(final String data) throws NoSuchAlgorithmException {
        return messageDigest.digest(data.getBytes(StandardCharsets.UTF_8));
    }

    public static Properties loadProperties(final String filepath) {
        final Properties streamsConfiguration = new Properties();

        try {
            streamsConfiguration.load(new FileReader(filepath));
        } catch (IOException e) {
            LOGGER.error(e.toString());
            System.exit(1);
            return null;
        }

        final String clusterInfo = streamsConfiguration.getProperty("cluster.info");
        if (clusterInfo != null) {
            final Properties clusterInfos = loadProperties(clusterInfo);

            streamsConfiguration.remove("cluster.info");
            streamsConfiguration.putAll(clusterInfos);
        }

        return streamsConfiguration;
    }

    public static Properties loadProperties(final String filename, final Class<?> rcsClass) {
        return loadProperties(rcsClass.getClassLoader().getResource(filename).getPath());
    }

    public static Map<String, Object> filterConfiguration(final Map<String, ?> properties, final String prefix) {
        final Map<String, Object> values = properties
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap((e) -> e.getKey().substring(prefix.length()), Map.Entry::getValue));

        if (values.containsKey("cluster.info")) {
            final String clusterInfo = (String) values.get("cluster.info");
            if (clusterInfo != null) {
                final Map<String, Object> clusterInfos = loadConfiguration(clusterInfo);

                properties.remove("cluster.info");
                values.putAll(clusterInfos);
            }
        }

        return values;
    }

    public static Map<String, Object> loadConfiguration(final String filepath) {
        final Properties streamsConfiguration = loadProperties(filepath);

        final Map<String, Object> config = new HashMap<>();
        streamsConfiguration.forEach((k, v) -> config.put(k.toString(), v));

        return config;
    }

    public static Map<String, Object> loadConfigurationFromResource(final String filename, final Class<?> rcsClass) {
        final Properties streamsConfiguration = loadProperties(filename, rcsClass);

        final Map<String, Object> config = new HashMap<>();
        streamsConfiguration.forEach((k, v) -> config.put(k.toString(), v));

        return config;
    }
}

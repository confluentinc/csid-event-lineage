package io.confluent.csid.data.governance.lineage.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static Properties loadProperties(final String filename, final Class<?> rcsClass) {
        final Properties streamsConfiguration = new Properties();
        try {
            streamsConfiguration.load(rcsClass.getClassLoader().getResourceAsStream(filename));
        } catch (IOException e) {
            LOGGER.error(e.toString());
            System.exit(1);
            return null;
        }

        return streamsConfiguration;
    }

    public static Map<String, Object> loadConfiguration(final Map<String, ?> properties, final String prefix) {
        return properties
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap((e) -> e.getKey().substring(prefix.length()), Map.Entry::getValue));
    }

    public static Map<String, Object> loadConfigurationFromFile(final String filename, final Class<?> rcsClass) {
        final Properties streamsConfiguration = loadProperties(filename, rcsClass);

        final Map<String, Object> config = new HashMap<>();
        streamsConfiguration.forEach((k, v) -> config.put(k.toString(), v));

        return config;
    }
}

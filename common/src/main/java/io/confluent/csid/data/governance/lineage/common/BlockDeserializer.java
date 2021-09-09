package io.confluent.csid.data.governance.lineage.common;

import io.confluent.csid.data.governance.lineage.common.rest.MinerRestClient;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@RequiredArgsConstructor()
public class BlockDeserializer <T, D extends Deserializer<T>> implements Deserializer<T> {
    private final static Logger LOGGER = LoggerFactory.getLogger(BlockDeserializer.class);
    private final static String MINER_HOST = "miner.url";

    @NonNull private final D deserializer;
    private MinerRestClient restClient;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.deserializer.configure(configs, isKey);

        final String minerURL = (String) configs.getOrDefault(MINER_HOST, null);
        if (minerURL == null || minerURL.isEmpty()) {
            LOGGER.error("No miner URL specified.");
            throw new SerializationException("No miner URL specified.");
        }

        restClient = new MinerRestClient(minerURL);
     }

    @Override
    public T deserialize(String s, byte[] bytes) {
        validate(bytes);
        return this.deserializer.deserialize(s, bytes);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        validate(data);
        return this.deserializer.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        this.deserializer.close();
    }

    private void validate(final byte[] data) {
        VerifyResponse isValid;
        try {
            isValid = restClient.isValid(data);
        } catch (Throwable e) {
            LOGGER.error("Error validating data.", e);
            throw new SerializationException("Error validating data.", e);
        }

        if (!isValid.isValid) {
            LOGGER.error("Data '{}' is not valid.", isValid.hash);
            throw new SerializationException(String.format("Data '%s' is not valid.", isValid.hash));
        }

        LOGGER.info("Data '{}' is valid.", isValid.hash);
    }
}

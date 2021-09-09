package io.confluent.csid.data.governance.lineage.producer;

import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    private KafkaProducer<String, String> producer;
    private boolean stop;

    public ProducerService() {
    }

    public void start() {
        LOGGER.info("Starting Producer service....");

        final Properties properties = BlockUtils.loadProperties("producer.properties", ProducerService.class);

        producer = new KafkaProducer<>(properties);

        final String outTopic = (String) properties.get("output.topic");
        int sent = 0;
        while(sent < 1 && !stop) {
            final String value = RandomStringUtils.randomAlphanumeric(17);
            producer.send(new ProducerRecord<>(outTopic, value, value));
            producer.flush();

            sent++;
        }

        LOGGER.info("Stream service started....");
    }

    public void stop() {
        LOGGER.info("Stopping Stream Service....");
        stop = true;
        producer.flush();
        producer.close();
        LOGGER.info("Stream service stopped....");
    }

}

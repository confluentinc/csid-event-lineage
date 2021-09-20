package io.confluent.csid.data.governance.lineage.miner.integration.producer;

import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    private final KafkaProducer<String, String> producer;

    public ProducerService(Properties brokerProps) {
        final Properties properties = BlockUtils.loadProperties("producer.properties", ProducerService.class);
        brokerProps.stringPropertyNames().forEach(key -> properties.setProperty(key, brokerProps.getProperty(key)));
        this.producer = new KafkaProducer<>(properties);
    }

    public void produceMessages(String topic, int numberToProduce) {
        final String prefix = RandomStringUtils.randomAlphanumeric(16);
        LOGGER.info("About to produce {} messages", numberToProduce);

        int msgsProducedCount = 0;
        while (msgsProducedCount < numberToProduce) {
            String msg = prefix + msgsProducedCount;
            producer.send(new ProducerRecord<>(topic, msg, msg));
            producer.flush();
            msgsProducedCount++;
        }
        LOGGER.info("Finished producing {} messages", numberToProduce);
    }

    public void close() {
        this.producer.close();
    }
}

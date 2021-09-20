package io.confluent.csid.data.governance.lineage.miner.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.csid.data.governance.lineage.common.Block;
import io.confluent.csid.data.governance.lineage.common.rest.MinerRestClient;
import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import io.confluent.csid.data.governance.lineage.miner.MinerRestService;
import io.confluent.csid.data.governance.lineage.miner.dao.impl.RockDBDAO;
import io.confluent.csid.data.governance.lineage.miner.integration.consumer.ConsumerService;
import io.confluent.csid.data.governance.lineage.miner.integration.producer.ProducerService;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;


@Slf4j
@Testcontainers
public class ProduceConsumeTest {

    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.2"))
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
        // try to speed up initial consumer group formation
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
        .withReuse(true);
    private Properties brokerProps;
    private final String minerServiceUrl = "http://localhost:1234";

    @BeforeAll
    static void prepareCluster() {
        kafkaContainer.start();
    }

    @AfterAll
    static void shutdownCluster() {
        kafkaContainer.stop();
    }

    @BeforeEach
    void setup() {
        brokerProps = new Properties();
        brokerProps.setProperty("bootstrap.servers", kafkaContainer.getBootstrapServers());
    }

    @Test
    @SneakyThrows
    void produceConsumeRestTest() {
        String topic = "produceConsumeRestTest_" + RandomStringUtils.randomAlphanumeric(10);
        //Prepare miner rest service and data store
        RockDBDAO dao = new RockDBDAO("rock");
        MinerRestService.getInstance().addStore(dao);
        MinerRestService.getInstance().start(new HostInfo("localhost", 1234));

        //prepare and subscribe consumer
        List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
        ConsumerService consumerService = new ConsumerService(brokerProps);
        consumerService.subscribe(topic);
        //prepare Producer
        ProducerService producerService = new ProducerService(brokerProps);

        //inject some messages
        producerService.produceMessages(topic, 10);
        //consume injected messages
        consumerService.poll(consumedRecords::add);

        //verify all messages were consumed
        assertThat(consumedRecords.size()).isEqualTo(10);

        //verify that service and DAO have record of those messages
        MinerRestClient minerRestClient = new MinerRestClient(minerServiceUrl);
        for (ConsumerRecord<String, String> record : consumedRecords) {
            byte[] bytesValue = record.value().getBytes(StandardCharsets.UTF_8);
            String dataHash = BlockUtils.getDataHash(bytesValue);
            Block storedBlock = minerRestClient.getBlock(bytesValue);
            assertThat(storedBlock.isValid()).isTrue();
            assertThat(dao.get(dataHash)).isEqualTo(storedBlock);
        }

        producerService.close();
        consumerService.close();
        dao.close();
        RocksDB.destroyDB("rock", new Options());
    }
}

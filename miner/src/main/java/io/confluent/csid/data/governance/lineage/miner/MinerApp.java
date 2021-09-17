package io.confluent.csid.data.governance.lineage.miner;

import io.confluent.csid.data.governance.lineage.miner.dao.impl.RockDBDAO;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class MinerApp  {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinerApp.class);

    public static void main(String[] args) {
        LOGGER.info("Starting miner app.");

        final CountDownLatch latch = new CountDownLatch(1);

        MinerRestService.getInstance().addStore(new RockDBDAO("/Users/pascal/Downloads/rock"));

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("miner-app-shutdown-hook") {
            @Override
            public void run() {
                try {
                    MinerRestService.getInstance().stop();
                } catch (Exception e) {
                    LOGGER.error("Error stopping web service.", e);
                }
                latch.countDown();
            }
        });

        try {
            LOGGER.info("Mining app started successfully.");

            MinerRestService.getInstance().start(new HostInfo("localhost", 1234));

            latch.await();
        } catch (Throwable e) {
            LOGGER.error("Mining app ended with error.", e);
            System.exit(1);
        }

        LOGGER.info("Mining app ended.");
        System.exit(0);
    }
}

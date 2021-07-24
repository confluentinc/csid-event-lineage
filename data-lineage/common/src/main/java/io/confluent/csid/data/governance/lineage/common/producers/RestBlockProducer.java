package io.confluent.csid.data.governance.lineage.common.producers;

import io.confluent.csid.data.governance.lineage.common.BlockProducer;
import io.confluent.csid.data.governance.lineage.common.rest.MinerRestClient;
import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import io.confluent.csid.data.governance.lineage.common.Block;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

@Slf4j
public class RestBlockProducer implements BlockProducer {
    private final static String BLOCK_PRODUCER_PREFIX = "block.producer";
    private final static String MINER_MINE = BLOCK_PRODUCER_PREFIX + ".mine";
    private final static String HOSTNAME = BLOCK_PRODUCER_PREFIX + ".hostname";
    private final static String PORT = BLOCK_PRODUCER_PREFIX + ".port";

    private MinerRestClient restClient;
    private boolean shouldMine;

    @Override
    public void sendBlock(byte[] data) {
        try {
            final Block block = new Block(data);
            if (shouldMine) {
                block.mineBlock(BlockUtils.prefix);
            }

            restClient.putBlock(block);
        } catch (NoSuchAlgorithmException | IOException e) {
            log.error("Error creating block.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        restClient = null;
    }

    @Override
    public void configure(Map<String, ?> map) {
        final String hostname = map.get(HOSTNAME).toString();
        final String port = map.get(PORT).toString();
        final String shouldMineStr = (String)map.get(MINER_MINE);
        shouldMine = (shouldMineStr != null && !shouldMineStr.isEmpty() && Boolean.parseBoolean(shouldMineStr));

        restClient = new MinerRestClient(hostname, (port != null) ? Integer.parseInt(port) : 1234);
    }
}

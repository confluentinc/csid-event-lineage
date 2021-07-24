package io.confluent.csid.data.governance.lineage.miner;

import io.confluent.csid.data.governance.lineage.common.Block;
import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import io.confluent.csid.data.governance.lineage.miner.dao.impl.KeyValueStoreDAO;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Miner implements Processor<String, Block> {
    private final static Logger LOGGER = LoggerFactory.getLogger(Miner.class);

    private ProcessorContext ctx;
    private KeyValueStore<String, Block> store;

    @Override
    public void init(ProcessorContext context) {
        this.ctx = context;
        this.store = (KeyValueStore<String, Block>) ctx.getStateStore(MinerService.STORE_NAME);

        try {
            MinerRestService.getInstance().addStore(new KeyValueStoreDAO(this.store));
            MinerRestService.getInstance().start(new HostInfo("localhost", 1234));
        } catch (Exception e) {
            LOGGER.error("Error starting rest proxy.", e);
        }
    }

    @Override
    public void process(String key, Block block) {
        if (!block.hasNonce()) {
            LOGGER.info("Mining block '{}'.", block.getData());

            // Mine
            block.mineBlock(BlockUtils.prefix);
        }

        // Save to the store
        store.put(block.getData(), block);

        // Forward
        ctx.forward(block.getData(), block);
    }

    @Override
    public void close() {
        try {
            MinerRestService.getInstance().stop();
        } catch (Exception e) {
            LOGGER.error("Error stopping rest proxy.", e);
        }
    }

}

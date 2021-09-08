package io.confluent.csid.data.governance.lineage.miner.dao.impl;

import io.confluent.csid.data.governance.lineage.common.Block;
import io.confluent.csid.data.governance.lineage.miner.dao.BlockDAO;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor()
public class KeyValueStoreDAO implements BlockDAO {

    @NonNull private final KeyValueStore<String, Block> store;

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public void put(Block block) {
        store.put(block.getData(), block);
    }

    @Override
    public Block get(String hash) {
        return store.get(hash);
    }
}

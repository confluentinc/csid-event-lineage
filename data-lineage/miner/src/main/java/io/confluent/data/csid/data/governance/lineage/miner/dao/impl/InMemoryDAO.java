package io.confluent.data.csid.data.governance.lineage.miner.dao.impl;

import io.confluent.data.lineage.Block;
import io.confluent.data.csid.data.governance.lineage.miner.dao.BlockDAO;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDAO implements BlockDAO {

    private final ConcurrentHashMap<String, Block> blocks = new ConcurrentHashMap<>();

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void put(Block block) {
        blocks.put(block.getHash(), block);
    }

    @Override
    public Block get(String hash) {
        return blocks.get(hash);
    }
}

package io.confluent.csid.data.governance.lineage.miner.dao.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.csid.data.governance.lineage.common.Block;
import io.confluent.csid.data.governance.lineage.miner.dao.BlockDAO;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class RockDBDAO implements BlockDAO {

    @NonNull private final RocksDB rocksDB;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public RockDBDAO(final String rootDir) {
        try {
            rocksDB = RocksDB.open(rootDir);
        } catch (RocksDBException e) {
            log.error("Error opening RockDB.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isOpen() {
        return rocksDB != null;
    }

    @Override
    public void put(Block block) {
        lock.writeLock().lock();
        try {
            rocksDB.put(block.getData().getBytes(StandardCharsets.UTF_8), new ObjectMapper().writeValueAsBytes(block));
            rocksDB.flush(new FlushOptions().setWaitForFlush(true));
        } catch (RocksDBException | JsonProcessingException e) {
            log.error("Error writing object to RockDB.", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Block get(String hash) {
        lock.readLock().lock();
        try {
            byte[] value = rocksDB.get(hash.getBytes(StandardCharsets.UTF_8));
            if (value != null) {
                return new ObjectMapper().readValue(value, Block.class);
            }

            log.warn("Block '{}' not found.", hash);
        } catch (RocksDBException | IOException e) {
            log.error("Error writing object to RockDB.", e);
        } finally {
            lock.readLock().unlock();
        }

        return null;
    }

    public void close() {
        rocksDB.close();
    }
}

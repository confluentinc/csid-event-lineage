package io.confluent.csid.data.governance.lineage.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.Date;

public class Block {
    private final static Logger LOGGER = LoggerFactory.getLogger(Block.class);
    private final static String GENESIS_HASH = "0";

    @JsonProperty("data")
    @Getter
    @Setter
    @NonNull private String data;
    @JsonProperty("previousHash")
    @Getter
    @NonNull private String previousHash;
    @JsonProperty("timestamp")
    @Getter
    private final long timeStamp = new Date().getTime();
    @JsonProperty("hash")
    @Getter
    private String hash;
    @JsonProperty("nonce")
    @Getter
    private int nonce;

    @JsonIgnore
    public boolean isGenesisBlock() {
        return previousHash.equals(GENESIS_HASH);
    }

    @JsonIgnore
    public boolean hasNonce() {
        return nonce != 0;
    }

    public Block() {}

    public Block(@NonNull  String data) {
        this(data, GENESIS_HASH);
    }

    public Block(@NonNull byte[] data) throws NoSuchAlgorithmException {
        this(data, "0");
    }

    public Block(@NonNull String data, @NonNull String previousHash) {
        this.data = data;
        this.previousHash = previousHash;
        this.hash = calculateBlockHash();
    }

    public Block(@NonNull byte[] data, @NonNull String previousHash) throws NoSuchAlgorithmException {
        this(BlockUtils.getDataHash(data), previousHash);
    }

    public String mineBlock(int prefix) {
        final String prefixString = new String(new char[prefix]).replace('\0', '0');

        while (!hash.substring(0, prefix).equals(prefixString)) {
            nonce++;
            hash = calculateBlockHash();
        }

        return hash;
    }

    public String calculateBlockHash() {
        final String dataToHash = previousHash + timeStamp + nonce + data;

        try {
            final byte[] bytes = BlockUtils.getDigest(dataToHash);

            StringBuilder buffer = new StringBuilder();
            for (byte b : bytes) {
                buffer.append(String.format("%02x", b));
            }

            return buffer.toString();
        } catch (NoSuchAlgorithmException ex) {
            LOGGER.error("Error calculating bock hash.", ex);
            throw new RuntimeException(ex);
        }
    }

    @JsonIgnore
    public boolean isValid() {
        return hash.equals(calculateBlockHash());
    }

}

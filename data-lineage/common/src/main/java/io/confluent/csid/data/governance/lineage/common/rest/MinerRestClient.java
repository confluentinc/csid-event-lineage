package io.confluent.csid.data.governance.lineage.common.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.csid.data.governance.lineage.common.VerifyResponse;
import io.confluent.csid.data.governance.lineage.common.utils.BlockUtils;
import io.confluent.csid.data.governance.lineage.common.Block;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;

public class MinerRestClient {

    private final String url;

    public MinerRestClient(String url) {
        this.url = url;
    }

    public MinerRestClient(String hostname, int port) {
        this(String.format("http://%s:%d", hostname, port));
    }

    public VerifyResponse isValid(final byte[] data) throws NoSuchAlgorithmException, IOException {
        final String hash = BlockUtils.getDataHash(data);
        return get("/miner/blocks/verify", "data", hash, VerifyResponse.class);
    }

    public Block getBlock(final byte[] data) throws NoSuchAlgorithmException, IOException {
        final String hash = BlockUtils.getDataHash(data);
        return get("/miner/blocks", hash, Block.class);
    }

    public Block putBlock(final Block block) throws IOException {
        return post("/miner/blocks", block);
    }

    private <T> T get(final String path, final String param, final String hash, final Class<T> tClass) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(String.format("%s/%s?%s=%s", url, path, param, hash, Charset.defaultCharset()));
        HttpResponse response = client.execute(request);

        return new ObjectMapper().readValue(response.getEntity().getContent(), tClass);
    }

    private <T> T get(final String path, final String hash, final Class<T> tClass) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(String.format("%s/%s/%s", url, path, URLEncoder.encode(hash, Charset.defaultCharset())));
        HttpResponse response = client.execute(request);

        return new ObjectMapper().readValue(response.getEntity().getContent(), tClass);
    }

    private Block post(final String path, Block block) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();

        HttpPost request = new HttpPost(String.format("%s/%s", url, path));
        request.setHeader("Accept", "application/json");
        request.setHeader("Content-type", "application/json");
        request.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(block)));

        HttpResponse response = client.execute(request);
        return new ObjectMapper().readValue(response.getEntity().getContent(), block.getClass());
    }
}

package io.confluent.data.csid.data.governance.lineage.miner;

import io.confluent.data.lineage.Block;
import io.confluent.data.lineage.VerifyResponse;
import io.confluent.data.csid.data.governance.lineage.miner.dao.BlockDAO;
import io.confluent.data.lineage.utils.BlockChainUtils;
import lombok.NonNull;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Path("miner")
public class MinerRestService {
    private final static Logger LOGGER = LoggerFactory.getLogger(MinerRestService.class);
    private final static MinerRestService instance = new MinerRestService();

    private Server jettyServer;
    private boolean started;

    private final List<BlockDAO> stores = new ArrayList<>();

    public static MinerRestService getInstance() {
        return instance;
    }

    private MinerRestService() {
    }

    @GET
    @Path("/blocks/verify")
    @Produces(MediaType.APPLICATION_JSON)
    public VerifyResponse verify(@QueryParam("data") final String data) {
        synchronized (this) {
            LOGGER.info("Receiving request to validate '{}'.", data);
            // Lookup block only for open stores
            final Block block = findBlock(data);
            return new VerifyResponse() {{
                isValid = block != null && BlockChainUtils.isValid(block, (hash) -> findBlock(hash));
                hash = data;
            }};
        }
    }

    @GET
    @Path("/blocks/{data}")
    @Produces(MediaType.APPLICATION_JSON)
    public Block get(@PathParam("data") final String data) {
        synchronized (this) {
            // Lookup block only for open stores
            final Block block = findBlock(data);
            if (block != null) {
                return block;
            }

            throw new NotFoundException();
        }
    }

    @POST
    @Path("/blocks/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Block put(@NonNull final Block block) {
        synchronized (this) {
            final int storeIndex = block.hashCode() % stores.size();
            final BlockDAO store = stores.get(storeIndex);
            store.put(block);

            return block;
        }
    }

    private Block findBlock(final String hash) {
        return stores.stream()
                .filter(BlockDAO::isOpen)
                .map(store -> store.get(hash))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * Start an embedded Jetty Server
     * @throws Exception from jetty
     */
    public void start(final HostInfo hostInfo) throws Exception {
        LOGGER.info("Starting web service.");

        if (started) {
            return;
        }

        synchronized (this) {
            if (started) {
                return;
            }

            final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");

            jettyServer = new Server();
            jettyServer.setHandler(context);

            final ResourceConfig rc = new ResourceConfig();
            rc.register(this);
            rc.register(JacksonFeature.class);

            final ServletContainer sc = new ServletContainer(rc);
            final ServletHolder holder = new ServletHolder(sc);
            context.addServlet(holder, "/*");

            final ServerConnector connector = new ServerConnector(jettyServer);
            connector.setHost(hostInfo.host());
            connector.setPort(hostInfo.port());
            jettyServer.addConnector(connector);

            context.start();

            try {
                jettyServer.start();
            } catch (final java.net.SocketException exception) {
                LOGGER.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
                throw new Exception(exception.toString());
            }

            started = true;
        }

        LOGGER.info("Web service started.");
    }

    /**
     * Stop the Jetty Server
     * @throws Exception from jetty
     */
    public void stop() throws Exception {
        if (!started) {
            return;
        }

        LOGGER.info("Stopping web service.");

        synchronized (this) {
            if (started) {
                if (jettyServer != null) {
                    jettyServer.stop();
                }

                started = false;
            }
        }

        LOGGER.info("Web service stopped.");
    }

    public void addStore(BlockDAO store) {
        synchronized (this) {
            stores.add(store);
        }
    }
}

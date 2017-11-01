package ru.mail.polis.lamtev;

import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;
import ru.mail.polis.lamtev.http_handlers.EntityHandler;
import ru.mail.polis.lamtev.http_handlers.InternalInteractionHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import static ru.mail.polis.lamtev.http_handlers.Utils.*;

public final class ClusteredKVService implements KVService {

    @NotNull
    private final HttpServer server;

    public ClusteredKVService(int port,
                              @NotNull final KVDAO dao,
                              @NotNull final Set<String> topology) throws IOException {
        final Cluster cluster = Cluster.of(topology);
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext(STATUS_PATH, http -> sendResponse(http, STATUS_RESPONSE, 200));
        server.createContext(ENTITY_PATH, new EntityHandler(cluster, dao));
        server.createContext(INTERNAL_INTERACTION_PATH, new InternalInteractionHandler(dao));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

}

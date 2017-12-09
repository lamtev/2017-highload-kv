package ru.mail.polis.lamtev;

import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;
import ru.mail.polis.lamtev.http_handlers.EntityHandler;
import ru.mail.polis.lamtev.http_handlers.InteractionBetweenNodesHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.*;

public final class ClusteredKVService implements KVService {

    //TODO make threads configurable
    private static final int N_THREADS = 5;

    @NotNull
    private final HttpServer server;
    @NotNull
    private final ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);
    @NotNull
    private final ExecutorService dataAccessPool = Executors.newSingleThreadExecutor();

    public ClusteredKVService(int port,
                              @NotNull final KVDAO dao,
                              @NotNull final Set<String> topology) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext(STATUS_PATH, http -> executor.execute(() -> {
            try {
                sendResponse(http, STATUS_RESPONSE, 200);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        server.createContext(ENTITY_PATH, http -> executor.execute(() -> {
            try {
                new EntityHandler(Cluster.of(topology)).handle(http);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
        server.createContext(INTERACTION_BETWEEN_NODES_PATH, http -> dataAccessPool.execute(() -> {
                    try {
                        new InteractionBetweenNodesHandler(dao).handle(http);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                })
        );
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

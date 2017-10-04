package ru.mail.polis.lamtev;

import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SimpleKVService implements KVService {

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String QUERY_PREFIX = "id=";
    private static final byte[] STATUS_RESPONSE = "ONLINE".getBytes();
    @NotNull
    private final HttpServer server;
    @NotNull
    private final KVDAO dao;

    public SimpleKVService(int port, @NotNull KVDAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        this.server.createContext(STATUS_PATH, http -> {
                    http.sendResponseHeaders(200, STATUS_RESPONSE.length);
                    http.getResponseBody().write(STATUS_RESPONSE);
                    http.close();
                }
        );

        this.server.createContext(ENTITY_PATH, http -> {
                    final String id = extractId(http.getRequestURI().getQuery());
                    switch (http.getRequestMethod()) {
                        case "GET": {
                            final byte[] value = dao.get(id);
                            http.sendResponseHeaders(200, value.length);
                            http.getResponseBody().write(value);
                            break;
                        }
                        case "PUT": {
                            final int contentLength = Integer.valueOf(http.getRequestHeaders().getFirst("Content-Length"));
                            final byte[] value = new byte[contentLength];
                            int readBytes = http.getRequestBody().read(value);
                            if (readBytes != contentLength) {
                                throw new IOException("Can't read at once");
                            }
                            dao.upsert(id, value);
                            http.sendResponseHeaders(201, 0);
                            break;
                        }
                        case "DELETE":
                            dao.delete(id);
                            http.sendResponseHeaders(202, 0);
                            break;
                        default:
                            http.sendResponseHeaders(405, 0);

                    }
                    http.close();
                }
        );
    }

    @NotNull
    private static String extractId(@NotNull final String query) {
        if (!query.startsWith(QUERY_PREFIX)) {
            throw new IllegalArgumentException("Shitty query");
        }
        return query.substring(QUERY_PREFIX.length());
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

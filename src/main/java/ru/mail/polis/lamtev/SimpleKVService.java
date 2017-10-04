package ru.mail.polis.lamtev;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

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
        this.server.createContext(ENTITY_PATH, this::processEntityRequest);
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

    private void processEntityRequest(HttpExchange http) throws IOException {
        final String id = extractId(http.getRequestURI().getQuery());
        switch (http.getRequestMethod()) {
            case "GET":
                processGetRequest(http, id);
                break;
            case "PUT":
                processPutRequest(http, id);
                break;
            case "DELETE":
                processDeleteRequest(http, id);
                break;
            default:
                http.sendResponseHeaders(405, 0);
        }
        http.close();
    }

    private void processDeleteRequest(HttpExchange http, String id) throws IOException {
        try {
            dao.delete(id);
        } catch (IllegalArgumentException e) {
            illegalId(http, e);
            return;
        }
        http.sendResponseHeaders(202, 0);
    }

    private void processPutRequest(HttpExchange http, String id) throws IOException {
        final int contentLength = Integer.valueOf(
                http.getRequestHeaders().getFirst("Content-Length"));
        final byte[] value = new byte[contentLength];
        int readBytes = http.getRequestBody().read(value);
        if (readBytes != contentLength) {
            throw new IOException("Can't read at once");
        }
        try {
            dao.upsert(id, value);
        } catch (IllegalArgumentException e) {
            illegalId(http, e);
            return;
        }
        http.sendResponseHeaders(201, 0);
    }

    private void processGetRequest(HttpExchange http, String id) throws IOException {
        final byte[] value;
        try {
            value = dao.get(id);
        } catch (NoSuchElementException e) {
            http.sendResponseHeaders(404, 0);
            return;
        } catch (IllegalArgumentException e) {
            illegalId(http, e);
            return;
        }
        http.sendResponseHeaders(200, value.length);
        http.getResponseBody().write(value);
    }

    private void illegalId(HttpExchange http, IllegalArgumentException e) throws IOException {
        final byte[] message = e.getMessage().getBytes();
        http.sendResponseHeaders(400, message.length);
        http.getResponseBody().write(message);
    }

}

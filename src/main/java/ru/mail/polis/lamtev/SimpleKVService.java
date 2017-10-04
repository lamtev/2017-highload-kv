package ru.mail.polis.lamtev;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public final class SimpleKVService implements KVService {

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String QUERY_PREFIX = "id=";
    private static final byte[] STATUS_RESPONSE = "ONLINE".getBytes();
    private static final String SHITTY_QUERY = "Shitty query";

    @NotNull
    private final HttpServer server;
    @NotNull
    private final KVDAO dao;

    public SimpleKVService(int port, @NotNull KVDAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        this.server.createContext(STATUS_PATH, this::processStatusRequest);
        this.server.createContext(ENTITY_PATH, this::processEntityRequest);
    }

    @NotNull
    private static String extractId(@NotNull final String query) {
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

    private void processStatusRequest(@NotNull HttpExchange http) throws IOException {
        http.sendResponseHeaders(200, STATUS_RESPONSE.length);
        http.getResponseBody().write(STATUS_RESPONSE);
        http.close();
    }

    private void processEntityRequest(@NotNull HttpExchange http) throws IOException {
        final String query = http.getRequestURI().getQuery();
        if (!query.startsWith(QUERY_PREFIX)) {
            sendResponse(http, SHITTY_QUERY, 400);
            http.close();
            return;
        }
        final String id = extractId(query);

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

    private void processGetRequest(@NotNull HttpExchange http, @NotNull String id) throws IOException {
        final byte[] value;
        try {
            value = dao.get(id);
        } catch (NoSuchElementException e) {
            sendResponse(http, e.getMessage(), 404);
            return;
        } catch (IllegalArgumentException e) {
            sendResponse(http, e.getMessage(), 400);
            return;
        }
        sendResponse(http, value, 200);
    }

    private void processPutRequest(@NotNull HttpExchange http, @NotNull String id) throws IOException {
        final int contentLength = Integer.valueOf(http.getRequestHeaders().getFirst("Content-Length"));
        final byte[] value = new byte[contentLength];
        if (http.getRequestBody().read(value) != value.length) {
            throw new IOException("Can't read at once");
        }
        try {
            dao.upsert(id, value);
        } catch (IllegalArgumentException e) {
            sendResponse(http, e.getMessage(), 400);
            return;
        }
        http.sendResponseHeaders(201, 0);
    }

    private void processDeleteRequest(@NotNull HttpExchange http, @NotNull String id) throws IOException {
        try {
            dao.delete(id);
        } catch (IllegalArgumentException e) {
            sendResponse(http, e.getMessage(), 400);
            return;
        }
        http.sendResponseHeaders(202, 0);
    }

    private void sendResponse(@NotNull HttpExchange http, @NotNull byte[] message, int code) throws IOException {
        http.sendResponseHeaders(code, message.length);
        http.getResponseBody().write(message);
    }

    private void sendResponse(@NotNull HttpExchange http, @NotNull String message, int code) throws IOException {
        sendResponse(http, message.getBytes(), code);
    }

}
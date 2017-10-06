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
    private static final String STATUS_RESPONSE = "ONLINE";
    private static final String SHITTY_QUERY = "Shitty query";
    private static final String CANT_READ_AT_ONCE = "Can't read at once";
    private static final String VALUE_BY_ID = "Value by id=";
    private static final String MIGHT_HAVE_BEEN_DELETED = "might have been deleted";
    private static final String HAVE_BEEN_UPDATED = "have been updated";
    private static final String NOT_ALLOWED = "not allowed";
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String CONTENT_LENGTH = "Content-Length";

    @NotNull
    private final HttpServer server;
    @NotNull
    private final KVDAO dao;

    public SimpleKVService(int port, @NotNull KVDAO dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        this.server.createContext(STATUS_PATH, http -> sendResponse(http, STATUS_RESPONSE, 200));
        this.server.createContext(ENTITY_PATH, this::processEntityRequest);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void processEntityRequest(@NotNull HttpExchange http) throws IOException {
        final String query = http.getRequestURI().getQuery();
        if (!query.startsWith(QUERY_PREFIX)) {
            sendResponse(http, SHITTY_QUERY, 400);
            return;
        }
        final String id = query.substring(QUERY_PREFIX.length());
        final String method = http.getRequestMethod();
        switch (method) {
            case GET:
                processGetRequest(http, id);
                break;
            case PUT:
                processPutRequest(http, id);
                break;
            case DELETE:
                processDeleteRequest(http, id);
                break;
            default:
                sendResponse(http, method + NOT_ALLOWED, 405);
        }
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
        final int contentLength = Integer.valueOf(http.getRequestHeaders().getFirst(CONTENT_LENGTH));
        final byte[] value = new byte[contentLength];
        if (contentLength != 0 && http.getRequestBody().read(value) != value.length) {
            throw new IOException(CANT_READ_AT_ONCE);
        }
        try {
            dao.upsert(id, value);
        } catch (IllegalArgumentException e) {
            sendResponse(http, e.getMessage(), 400);
            return;
        }
        sendResponse(http, VALUE_BY_ID + id + HAVE_BEEN_UPDATED, 201);
    }

    private void processDeleteRequest(@NotNull HttpExchange http, @NotNull String id) throws IOException {
        try {
            dao.delete(id);
        } catch (IllegalArgumentException e) {
            sendResponse(http, e.getMessage(), 400);
            return;
        }
        sendResponse(http, VALUE_BY_ID + id + MIGHT_HAVE_BEEN_DELETED, 202);
    }

    private void sendResponse(@NotNull HttpExchange http, @NotNull byte[] message, int code) throws IOException {
        http.sendResponseHeaders(code, message.length);
        http.getResponseBody().write(message);
        http.close();
    }

    private void sendResponse(@NotNull HttpExchange http, @NotNull String message, int code) throws IOException {
        sendResponse(http, message.getBytes(), code);
    }

}
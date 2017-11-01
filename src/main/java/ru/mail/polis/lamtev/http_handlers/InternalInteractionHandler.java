package ru.mail.polis.lamtev.http_handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.lamtev.KVDAO;

import java.io.IOException;
import java.util.NoSuchElementException;

import static ru.mail.polis.lamtev.http_handlers.Utils.*;

//TODO make internal interaction secure
public class InternalInteractionHandler implements HttpHandler {

    @NotNull
    private final KVDAO dao;

    public InternalInteractionHandler(@NotNull KVDAO dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
        final String query = http.getRequestURI().getQuery();
        if (!query.startsWith(QUERY_PREFIX)) {
            sendResponse(http, SHITTY_QUERY, 400);
            return;
        }
        final QueryParser parser = new QueryParser(query);
        final String id = parser.id();

        final String method = http.getRequestMethod();
        switch (method) {
            case GET:
                handleGetRequest(http, id);
                break;
            case PUT:
                handlePutRequest(http, id);
                break;
            case DELETE:
                handleDeleteRequest(http, id);
                break;
            default:
                sendResponse(http, method + NOT_ALLOWED, 405);
        }
    }

    private void handleGetRequest(@NotNull HttpExchange http, @NotNull String id) throws IOException {
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

    private void handlePutRequest(@NotNull HttpExchange http, @NotNull String id) throws IOException {
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

    private void handleDeleteRequest(@NotNull HttpExchange http, @NotNull String id) throws IOException {
        try {
            dao.delete(id);
        } catch (IllegalArgumentException e) {
            sendResponse(http, e.getMessage(), 400);
            return;
        }
        sendResponse(http, VALUE_BY_ID + id + MIGHT_HAVE_BEEN_DELETED, 202);
    }

}

package ru.mail.polis.lamtev.http_handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.lamtev.KVDAO;

import java.io.IOException;
import java.util.NoSuchElementException;

import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.*;

//TODO make internal interaction secure
public final class InteractionBetweenNodesHandler implements HttpHandler {

    @NotNull
    private final KVDAO dao;
    @SuppressWarnings("NullableProblems")
    @NotNull
    private HttpExchange http;

    public InteractionBetweenNodesHandler(@NotNull KVDAO dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
        this.http = http;
        final String query = http.getRequestURI().getQuery();
        final QueryParams params = parseQuery(query);
        final String id = params.id();

        final String method = http.getRequestMethod();
        switch (method) {
            case GET:
                handleGetRequest(id);
                break;
            case PUT:
                handlePutRequest(id);
                break;
            case DELETE:
                handleDeleteRequest(id, params.deleteDeletedId());
                break;
            default:
                sendResponse(http, method + NOT_ALLOWED, 405);
        }
    }

    private void handleGetRequest(@NotNull String id) throws IOException {
        final byte[] value;
        try {
            value = dao.get(id);
        } catch (NoSuchElementException e) {
            sendResponse(http, e.getMessage(), 404);
            return;
        }
        sendResponse(http, value, 200);
    }

    private void handlePutRequest(@NotNull String id) throws IOException {
        final byte[] value = readData(http.getRequestBody());
        dao.upsert(id, value);
        sendResponse(http, VALUE_BY_ID + id + HAVE_BEEN_UPDATED, 201);
    }

    private void handleDeleteRequest(@NotNull String id, boolean deleteDeletedId) throws IOException {
        if (!deleteDeletedId) {
            dao.delete(id);
            sendResponse(http, VALUE_BY_ID + id + MIGHT_HAVE_BEEN_DELETED, 202);
        } else {
            dao.deleteDeletedId(id);
            sendResponse(http, DELETED_ID + id + MIGHT_HAVE_BEEN_DELETED, 202);
        }
    }

}

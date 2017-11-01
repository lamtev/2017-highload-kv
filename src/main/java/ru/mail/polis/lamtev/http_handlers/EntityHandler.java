package ru.mail.polis.lamtev.http_handlers;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.lamtev.Cluster;
import ru.mail.polis.lamtev.KVDAO;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.mail.polis.lamtev.http_handlers.Utils.*;

//TODO
public class EntityHandler implements HttpHandler {

    @NotNull
    private final Cluster cluster;
    @NotNull
    private final KVDAO dao;
    @NotNull
    private HttpExchange http;

    public EntityHandler(@NotNull Cluster cluster, @NotNull KVDAO dao) {
        this.cluster = cluster;
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
        this.http = http;
        final String query = http.getRequestURI().getQuery();
        if (!query.startsWith(QUERY_PREFIX)) {
            sendResponse(http, SHITTY_QUERY, 400);
            return;
        }
        final QueryParser parser = new QueryParser(query);
        final String id = parser.id();
        final int ack = parser.ack() == 0 ? cluster.quorum() : parser.ack();
        final int from = parser.from() == 0 ? cluster.numberOfNodes() : parser.from();
        final String method = http.getRequestMethod();
        final List<String> nodes = cluster.nodes(id, from);
        switch (method) {
            case GET:
                handleGetRequest(id, ack, from, nodes);
                break;
            case PUT:
                handlePutRequest(id, ack, from, nodes);
                break;
            case DELETE:
                handleDeleteRequest(id, ack, from, nodes);
                break;
            default:
                sendResponse(http, method + NOT_ALLOWED, 405);
        }
    }

    private void handleGetRequest(@NotNull String id, int ack, int from, @NotNull List<String> nodes) throws IOException {

    }

    private void handlePutRequest(@NotNull String id, int ack, int from, @NotNull List<String> nodes) throws IOException {
        AtomicInteger counter = new AtomicInteger(0);
        List<Future<HttpResponse<InputStream>>> futures = new ArrayList<>();
        for (String node : nodes) {
            Future<HttpResponse<InputStream>> future = Unirest.put(node + INTERNAL_INTERACTION_PATH + "?id=" + id)
                    .routeParam("id", id)
                    .body(http.getRequestBody())
                    .asBinaryAsync();
            futures.add(future);
        }

        futures.forEach(it -> {
            try {
                HttpResponse<InputStream> response = it.get();
                if (response.getStatus() == 200) {
                    counter.incrementAndGet();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

        if (counter.get() >= ack) {
            sendResponse(http, "ok", 201);
        } else {
            sendResponse(http, "not ok", 504);
        }

    }

    private void handleDeleteRequest(@NotNull String id, int ack, int from, @NotNull List<String> nodes) throws IOException {

    }

}

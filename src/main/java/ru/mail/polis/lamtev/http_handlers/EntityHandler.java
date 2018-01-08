package ru.mail.polis.lamtev.http_handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.http.HttpResponse;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.lamtev.Cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toList;
import static ru.mail.polis.lamtev.FileKVDAO.DELETED;
import static ru.mail.polis.lamtev.http_handlers.Client.*;
import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.*;

//TODO logging
public final class EntityHandler implements HttpHandler {

    private static final long TIMEOUT = 150L;
    @NotNull
    private final Cluster cluster;
    @NotNull
    private final ExecutorService poolOfRequestsToNodes;
    @SuppressWarnings("NullableProblems")
    @NotNull
    private HttpExchange http;

    public EntityHandler(@NotNull Cluster cluster) {
        this.cluster = cluster;
        this.poolOfRequestsToNodes = Executors.newFixedThreadPool(cluster.numberOfNodes());
    }

    @Override
    public void handle(@NotNull HttpExchange http) {
        this.http = http;
        final String query = http.getRequestURI().getQuery();
        if (queryIsInvalid(query)) {
            return;
        }
        final QueryParams queryParams = parseQuery(query);
        if (queryParamsAreIncorrect(queryParams)) {
            return;
        }
        final String id = queryParams.id();
        final int ack = queryParams.ack() == 0 ? cluster.quorum() : queryParams.ack();
        final int from = queryParams.from() == 0 ? cluster.numberOfNodes() : queryParams.from();
        final List<String> nodes = cluster.nodes(id, from);
        final String method = http.getRequestMethod();
        switch (method) {
            case GET:
                handleGetRequest(id, ack, nodes);
                break;
            case PUT:
                handlePutRequest(id, ack, nodes);
                break;
            case DELETE:
                handleDeleteRequest(id, ack, nodes);
                break;
            default:
                sendResponse(http, method + NOT_ALLOWED, 405);
                break;
        }
    }

    private void handleGetRequest(@NotNull String id, int ack, @NotNull List<String> nodes) {
        final List<CompletableFuture<HttpResponse>> futures = sendGetRequests(nodes, id);

        final AtomicInteger nOk = new AtomicInteger(0);
        final AtomicInteger nNotFound = new AtomicInteger(0);
        final AtomicInteger nDeleted = new AtomicInteger(0);
        final AtomicReference<List<byte[]>> values = new AtomicReference<>(new ArrayList<>());

        futures.forEach(future -> future.thenAccept(response -> {
            try {
                final int statusCode = response.getStatusLine().getStatusCode();
                final byte[] value = readData(response.getEntity().getContent());
                switch (statusCode) {
                    case 200:
                        nOk.incrementAndGet();
                        values.get().add(value);
                        break;
                    case 404:
                        if (DELETED.equals(new String(value))) {
                            nDeleted.incrementAndGet();
                        } else {
                            nNotFound.incrementAndGet();
                        }
                        break;
                }
            } catch (IOException ignored) {
            }
        }));

        futureAllOfWithinATimeout(futures, TIMEOUT).thenRun(() -> {
            if (nOk.get() + nNotFound.get() + nDeleted.get() < ack) {
                sendResponse(http, NOT_ENOUGH_REPLICAS, 504);
            } else if (nDeleted.get() > 0 || nNotFound.get() >= ack) {
                sendResponse(http, NOT_FOUND, 404);
            } else {
                sendResponse(http, consistentValue(values.get(), ack), 200);
            }
        });
    }

    @NotNull
    private List<CompletableFuture<HttpResponse>> sendGetRequests(@NotNull List<String> nodes, @NotNull String id) {
        return nodes.stream()
                .map(it ->
                        CompletableFuture.supplyAsync(() -> sendGetRequest(it, id), poolOfRequestsToNodes)
                                .exceptionally(e -> EXCEPTION_HTTP_RESPONSE)
                )
                .collect(toList());

    }

    private void handlePutRequest(@NotNull String id, int ack, @NotNull List<String> nodes) {
        final byte[] data = readData(http.getRequestBody());
        final List<CompletableFuture<HttpResponse>> futures = sendPutRequests(nodes, id, data);

        final AtomicInteger nCreated = new AtomicInteger(0);
        final AtomicInteger nIllegal = new AtomicInteger(0);

        futures.forEach(future -> future.thenAccept(response -> {
            final int statusCode = response.getStatusLine().getStatusCode();
            switch (statusCode) {
                case 201:
                    nCreated.incrementAndGet();
                    break;
                case 400:
                    nIllegal.incrementAndGet();
                    break;
            }
        }));

        futureAllOfWithinATimeout(futures, TIMEOUT).thenRun(() -> {
            if (nCreated.get() < ack) {
                sendResponse(http, NOT_ENOUGH_REPLICAS, 504);
            } else {
                sendResponse(http, CREATED, 201);
            }
        });
    }

    @NotNull
    private List<CompletableFuture<HttpResponse>> sendPutRequests(@NotNull List<String> nodes,
                                                                  @NotNull String id, byte[] data) {
        return nodes.stream()
                .map(it ->
                        CompletableFuture.supplyAsync(() -> sendPutRequest(it, id, data), poolOfRequestsToNodes)
                                .exceptionally(e -> EXCEPTION_HTTP_RESPONSE)
                )
                .collect(toList());
    }

    private void handleDeleteRequest(@NotNull String id, int ack, @NotNull List<String> nodes) {
        final List<CompletableFuture<HttpResponse>> futures = sendDeleteRequests(id, nodes);

        final AtomicInteger nAccepted = new AtomicInteger(0);
        final AtomicInteger nIllegal = new AtomicInteger(0);

        futures.forEach(future -> future.thenAccept(response -> {
            final int statusCode = response.getStatusLine().getStatusCode();
            switch (statusCode) {
                case 202:
                    nAccepted.incrementAndGet();
                    break;
                case 400:
                    nIllegal.incrementAndGet();
                    break;
            }
        }));

        futureAllOfWithinATimeout(futures, TIMEOUT).thenRun(() -> {
            if (nAccepted.get() < ack) {
                sendResponse(http, NOT_ENOUGH_REPLICAS, 504);
            } else {
                sendResponse(http, MIGHT_HAVE_BEEN_DELETED, 202);
            }
            attemptDeleteUnnecessaryDeletedId(nodes, id);
        });
    }

    @NotNull
    private List<CompletableFuture<HttpResponse>> sendDeleteRequests(@NotNull String id, @NotNull List<String> nodes) {
        return nodes.stream()
                .map(it ->
                        CompletableFuture.supplyAsync(() -> sendDeleteRequest(it, id), poolOfRequestsToNodes)
                                .exceptionally(e -> EXCEPTION_HTTP_RESPONSE)
                )
                .collect(toList());
    }

    private void attemptDeleteUnnecessaryDeletedId(@NotNull List<String> nodes, @NotNull String id) {
        final List<CompletableFuture<HttpResponse>> futures = sendGetRequests(nodes, id);

        final AtomicInteger nDeleted = new AtomicInteger(0);

        futures.forEach(future -> future.thenAccept(response -> {
            try {
                final int statusCode = response.getStatusLine().getStatusCode();
                final byte[] value = readData(response.getEntity().getContent());
                if (statusCode == 404 && DELETED.equals(new String(value))) {
                    nDeleted.incrementAndGet();
                }
            } catch (IOException ignored) {
            }
        }));

        futureAllOfWithinATimeout(futures, TIMEOUT).thenRun(() -> {
            if (nDeleted.get() == cluster.numberOfNodes()) {
                deleteDeletedIds(nodes, id);
            }
        });
    }

    private void deleteDeletedIds(@NotNull List<String> nodes, @NotNull String id) {
        nodes.forEach(
                node -> CompletableFuture.supplyAsync(
                        () -> sendDeleteRequest(node, id + DELETE_DELETED_ID_TRUE),
                        poolOfRequestsToNodes
                )
        );
    }

    private boolean queryIsInvalid(@NotNull String query) {
        if (!query.startsWith(QUERY_PREFIX)) {
            sendResponse(http, QUERY_IS_INVALID, 400);
            return true;
        }
        return false;
    }

    private boolean queryParamsAreIncorrect(@NotNull QueryParams params) {
        if (params.id().isEmpty()) {
            sendResponse(http, ILLEGAL_ID, 400);
            return true;
        }
        final int ack = params.ack();
        final int from = params.from();
        if (ack == -1 || from == -1) {
            sendResponse(http, TOO_SMALL_RF, 400);
            return true;
        }
        if (ack > from || ack > cluster.numberOfNodes() || from > cluster.numberOfNodes()) {
            sendResponse(http, TOO_BIG_RF, 400);
            return true;
        }
        return false;
    }

}

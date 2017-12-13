package ru.mail.polis.lamtev.http_handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.lamtev.Cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.mail.polis.lamtev.FileKVDAO.DELETED;
import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.*;

//TODO logging
public final class EntityHandler implements HttpHandler {

    @NotNull
    private final Cluster cluster;
    @NotNull
    private final ExecutorService poolOfRequestsToNodes;
    @NotNull
    private HttpExchange http;

    public EntityHandler(@NotNull Cluster cluster) {
        this.cluster = cluster;
        this.poolOfRequestsToNodes = Executors.newFixedThreadPool(cluster.numberOfNodes());
    }

    @Override
    public void handle(@NotNull HttpExchange http) throws IOException {
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
        }
    }

    private void handleGetRequest(@NotNull String id, int ack, @NotNull List<String> nodes) throws IOException {
        final List<Future<HttpResponse>> futures = nodes.parallelStream()
                .map(node -> poolOfRequestsToNodes.submit(
                        () -> Request.Get(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                                .execute()
                                .returnResponse()
                ))
                .collect(Collectors.toList());

        final AtomicInteger nOk = new AtomicInteger(0);
        final AtomicInteger nNotFound = new AtomicInteger(0);
        final AtomicInteger nDeleted = new AtomicInteger(0);
        final List<byte[]> values = new ArrayList<>();

        for (Future<HttpResponse> future : futures) {
            try {
                final HttpResponse response = future.get(300L, MILLISECONDS);
                final int statusCode = response.getStatusLine().getStatusCode();
                final byte[] value = readData(response.getEntity().getContent());
                switch (statusCode) {
                    case 200:
                        nOk.incrementAndGet();
                        values.add(value);
                        break;
                    case 404:
                        final String msg = new String(value);
                        if (DELETED.equals(msg)) {
                            nDeleted.incrementAndGet();
                        } else {
                            nNotFound.incrementAndGet();
                        }
                        break;
                    default:
                        throw new TimeoutException();
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                future.cancel(true);
            }
        }

        if (nOk.get() + nNotFound.get() + nDeleted.get() < ack) {
            sendResponse(http, NOT_ENOUGH_REPLICAS, 504);
        } else if (nDeleted.get() > 0 || nNotFound.get() >= ack) {
            if (nDeleted.get() == ack) {
                deleteDeletedIds(nodes, id);
            }
            sendResponse(http, NOT_FOUND, 404);
        } else {
            sendResponse(http, consistentValue(values, ack), 200);
        }
    }

    private void deleteDeletedIds(@NotNull List<String> nodes, @NotNull String id) {
        nodes.parallelStream().forEach(node ->
                poolOfRequestsToNodes.submit(
                        () -> Request.Delete(node + INTERACTION_BETWEEN_NODES_PATH + ID + id + DELETE_DELETED_ID_TRUE)
                                .execute()
                                .returnResponse()
                )
        );
    }

    private void handlePutRequest(@NotNull String id, int ack, @NotNull List<String> nodes) throws IOException {
        final byte[] data = readData(http.getRequestBody());

        final List<Future<HttpResponse>> futures = nodes.parallelStream()
                .map(node -> poolOfRequestsToNodes.submit(
                        () -> Request.Put(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                                .bodyByteArray(data)
                                .execute()
                                .returnResponse()
                ))
                .collect(Collectors.toList());

        final AtomicInteger nCreated = new AtomicInteger(0);
        final AtomicInteger nIllegal = new AtomicInteger(0);

        futures.forEach(future -> {
            try {
                final int statusCode = future.get(300L, MILLISECONDS).getStatusLine().getStatusCode();
                switch (statusCode) {
                    case 201:
                        nCreated.incrementAndGet();
                        break;
                    case 400:
                        nIllegal.incrementAndGet();
                        break;
                    default:
                        throw new TimeoutException();
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                future.cancel(true);
            }
        });

        if (nCreated.get() < ack) {
            sendResponse(http, NOT_ENOUGH_REPLICAS, 504);
        } else {
            sendResponse(http, CREATED, 201);
        }

    }

    private void handleDeleteRequest(@NotNull String id, int ack, @NotNull List<String> nodes) throws IOException {
        final List<Future<HttpResponse>> futures = nodes.parallelStream()
                .map(node -> poolOfRequestsToNodes.submit(
                        () -> Request.Delete(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                                .execute()
                                .returnResponse()
                ))
                .collect(Collectors.toList());

        final AtomicInteger nAccepted = new AtomicInteger(0);
        final AtomicInteger nIllegal = new AtomicInteger(0);

        futures.forEach(future -> {
            try {
                final int statusCode = future.get(300L, MILLISECONDS).getStatusLine().getStatusCode();
                switch (statusCode) {
                    case 202:
                        nAccepted.incrementAndGet();
                        break;
                    case 400:
                        nIllegal.incrementAndGet();
                        break;
                    default:
                        throw new TimeoutException();
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                future.cancel(true);
            }
        });

        if (nAccepted.get() < ack) {
            sendResponse(http, NOT_ENOUGH_REPLICAS, 504);
        } else {
            sendResponse(http, MIGHT_HAVE_BEEN_DELETED, 202);
        }
    }

    private boolean queryIsInvalid(@NotNull String query) throws IOException {
        if (!query.startsWith(QUERY_PREFIX)) {
            sendResponse(http, QUERY_IS_INVALID, 400);
            return true;
        }
        return false;
    }

    private boolean queryParamsAreIncorrect(@NotNull QueryParams params) throws IOException {
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

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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.mail.polis.lamtev.http_handlers.Utils.*;

//TODO
public class EntityHandler implements HttpHandler {

    @NotNull
    private final Cluster cluster;
    @NotNull
    private final ExecutorService putExecutor;
    @NotNull
    private HttpExchange http;

    public EntityHandler(@NotNull Cluster cluster) {
        this.cluster = cluster;
        this.putExecutor = Executors.newFixedThreadPool(cluster.numberOfNodes());
    }

    @Override
    public void handle(@NotNull HttpExchange http) throws IOException {
        this.http = http;
        final String query = http.getRequestURI().getQuery();
        if (!query.startsWith(QUERY_PREFIX)) {
            sendResponse(http, SHITTY_QUERY, 400);
            return;
        }
        final QueryParser parser = new QueryParser(query);
        if (parser.id().isEmpty()) {
            sendResponse(http, "Illegal id", 400);
            return;
        }
        if (parser.ack() == -1 || parser.from() == -1) {
            sendResponse(http, "To small RF", 400);
            return;
        }
        final String id = parser.id();
        final int ack = parser.ack() == 0 ? cluster.quorum() : parser.ack();
        final int from = parser.from() == 0 ? cluster.numberOfNodes() : parser.from();
        if (ack > from || ack > cluster.numberOfNodes() || from > cluster.numberOfNodes()) {
            sendResponse(http, "To big RF", 400);
            return;
        }
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
        final List<Future<HttpResponse>> futures = new ArrayList<>();

        nodes.parallelStream().forEach(it -> futures.add(
                putExecutor.submit(() -> Request.Get(it + INTERNAL_INTERACTION_PATH + "?id=" + id)
                        .execute()
                        .returnResponse()
                )
        ));

        final AtomicInteger nOk = new AtomicInteger(0);
        final AtomicInteger nNotFound = new AtomicInteger(0);
        final AtomicInteger nDeleted = new AtomicInteger(0);
        final AtomicInteger nIllegal = new AtomicInteger(0);
        final List<byte[]> values = new ArrayList<>();

        for (Future<HttpResponse> future : futures) {
            try {
                final HttpResponse response = future.get(300L, MILLISECONDS);
                final int statusCode = response.getStatusLine().getStatusCode();
                switch (statusCode) {
                    case 200:
                        nOk.incrementAndGet();
                        values.add(readData(response.getEntity().getContent()));
                        break;
                    case 404:
                        final byte[] msgBytes = readData(response.getEntity().getContent());
                        final String msg = new String(msgBytes);
                        if ("DELETED".equals(msg)) {
                            nDeleted.incrementAndGet();
                        } else {
                            nNotFound.incrementAndGet();
                        }
                        break;
                    case 400:
                        nIllegal.incrementAndGet();
                        break;
                    default:

                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                future.cancel(true);
            }
        }

        if (nOk.get() + nNotFound.get() + nDeleted.get() < ack) {
            sendResponse(http, "Not enough replicas", 504);
        } else if (nDeleted.get() > 0 || nNotFound.get() >= ack) {
            sendResponse(http, "Not found", 404);
        } else {
            sendResponse(http, consistentValue(values, ack), 200);
        }
    }

    private void handlePutRequest(@NotNull String id, int ack, @NotNull List<String> nodes) throws IOException {
        final List<Future<HttpResponse>> futures = new ArrayList<>();

        final byte[] data = readData(http.getRequestBody());
        nodes.parallelStream().forEach(it -> futures.add(
                putExecutor.submit(() -> Request.Put(it + INTERNAL_INTERACTION_PATH + "?id=" + id)
                        .bodyByteArray(data)
                        .execute()
                        .returnResponse())
        ));

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
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                future.cancel(true);
            }
        });

        if (nCreated.get() < ack) {
            sendResponse(http, "Not enough replicas", 504);
        } else {
            sendResponse(http, "Created", 201);
        }

    }

    private void handleDeleteRequest(@NotNull String id, int ack, @NotNull List<String> nodes) throws IOException {
        final List<Future<HttpResponse>> futures = new ArrayList<>();

        nodes.parallelStream().forEach(it -> {
            Future<HttpResponse> response = putExecutor.submit(
                    () -> Request.Delete(it + INTERNAL_INTERACTION_PATH + "?id=" + id).execute().returnResponse()
            );
            futures.add(response);
        });

        final AtomicInteger nAccepted = new AtomicInteger(0);
        final AtomicInteger nIllegal = new AtomicInteger(0);

        for (Future<HttpResponse> future : futures) {
            try {
                final int statusCode = future.get(300L, MILLISECONDS).getStatusLine().getStatusCode();
                switch (statusCode) {
                    case 202:
                        nAccepted.incrementAndGet();
                        break;
                    case 400:
                        nIllegal.incrementAndGet();
                        break;
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                future.cancel(true);
            }
        }

        if (nAccepted.get() < ack) {
            sendResponse(http, "Not enough replicas", 504);
        } else {
            sendResponse(http, MIGHT_HAVE_BEEN_DELETED, 202);
        }
    }

}

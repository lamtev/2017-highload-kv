package ru.mail.polis.lamtev.http_handlers;

import com.sun.net.httpserver.HttpExchange;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.CompletableFuture.completedFuture;

public final class HandlerUtils {

    public static final String STATUS_PATH = "/v0/status";
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String INTERACTION_BETWEEN_NODES_PATH = "/v0/internal-connection";
    public static final String STATUS_RESPONSE = "ONLINE";
    static final String QUERY_PREFIX = "id=";
    static final String QUERY_IS_INVALID = "Query is invalid";
    static final String VALUE_BY_ID = "Value by id=";
    static final String MIGHT_HAVE_BEEN_DELETED = "might have been deleted";
    static final String HAVE_BEEN_UPDATED = "have been updated";
    static final String NOT_ALLOWED = "not allowed";
    static final String GET = "GET";
    static final String PUT = "PUT";
    static final String DELETE = "DELETE";
    static final String NOT_ENOUGH_REPLICAS = "Not enough replicas";
    static final String NOT_FOUND = "Not found";
    static final String ID = "?id=";
    static final String CREATED = "Created";
    static final String ILLEGAL_ID = "Illegal id";
    static final String TOO_SMALL_RF = "Too small RF";
    static final String TOO_BIG_RF = "Too big RF";
    static final String DELETED_ID = "Deleted id=";
    static final String DELETE_DELETED_ID_TRUE = "&deleteDeletedId=true";
    private static final String ID_PREFIX = "id=";
    private static final String REPLICAS_PREFIX = "&replicas=";
    private static final String DELETE_DELETED_IDS = "&deleteDeletedId=";

    private HandlerUtils() {
        throw new UnsupportedOperationException();
    }

    static void sendResponse(@NotNull HttpExchange http, @NotNull byte[] message, int code) {
        try {
            http.sendResponseHeaders(code, message.length);
            http.getResponseBody().write(message);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        http.close();
    }

    public static void sendResponse(@NotNull HttpExchange http, @NotNull String message, int code) {
        sendResponse(http, message.getBytes(), code);
    }

    @NotNull
    static byte[] readData(@NotNull InputStream is) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final byte[] buffer = new byte[1024];
            for (int len; (len = is.read(buffer, 0, 1024)) != -1; ) {
                os.write(buffer, 0, len);
            }
            os.flush();
            return os.toByteArray();
        }
    }

    @NotNull
    static byte[] consistentValue(@NotNull List<byte[]> values, int ack) {
        for (byte[] value : values) {
            final int cnt = (int) values.stream()
                    .filter(it -> Arrays.equals(it, value))
                    .count();
            if (cnt == ack) {
                return value;
            }
        }
        return values.get(0);
    }

    @NotNull
    static QueryParams parseQuery(@NotNull String query) {
        final String id;
        final int ack;
        final int from;
        final boolean deleteDeletedId;
        if (query.contains(REPLICAS_PREFIX)) {
            id = query.substring(ID_PREFIX.length(), query.indexOf(REPLICAS_PREFIX));
            final String[] replicas = query.substring(query.indexOf(REPLICAS_PREFIX) + REPLICAS_PREFIX.length()).split("/");
            final int mbAck = Integer.valueOf(replicas[0]);
            ack = mbAck > 0 ? mbAck : -1;
            final int mbFrom = Integer.valueOf(replicas[1]);
            from = mbFrom > 0 ? mbFrom : -1;
            deleteDeletedId = false;
        } else if (query.contains(DELETE_DELETED_IDS)) {
            id = query.substring(ID_PREFIX.length(), query.indexOf(DELETE_DELETED_IDS));
            ack = from = 0;
            deleteDeletedId = Boolean.valueOf(query.substring(query.indexOf(DELETE_DELETED_IDS) + DELETE_DELETED_IDS.length()));
        } else {
            id = query.substring(ID_PREFIX.length());
            ack = from = 0;
            deleteDeletedId = false;
        }
        return new QueryParams() {
            @NotNull
            @Override
            public String id() {
                return id;
            }

            @Override
            public int ack() {
                return ack;
            }

            @Override
            public int from() {
                return from;
            }

            @Override
            public boolean deleteDeletedId() {
                return deleteDeletedId;
            }
        };
    }

    /** Returns a completedFuture of list {@code List<CompletableFuture<T>>}
     * of those futures {@see cfs} that has been completed within allotted
     * timeout {@code timeoutMillis}
     *
     * @param cfs list of futures
     * @param timeoutMillis timeout in millis
     * @param <T> T
     * @return completedFuture of list which consists of those futures {@code cfs}
     * that has been completed within allotted timeout {@code timeoutMillis}
     */
    @NotNull
    static <T> CompletableFuture<List<CompletableFuture<T>>> futureAllOfWithinATimeout(
            @NotNull List<CompletableFuture<T>> cfs, long timeoutMillis) {
        final List<CompletableFuture<T>> list = new ArrayList<>();
        cfs.parallelStream().forEach(future -> {
            try {
                list.add(completedFuture(future.get(timeoutMillis, TimeUnit.MILLISECONDS)));
            } catch (InterruptedException | ExecutionException | TimeoutException ignored) {
            }
        });
        return completedFuture(list);
    }

    public interface QueryParams {
        @NotNull
        String id();

        int ack();

        int from();

        boolean deleteDeletedId();
    }

}

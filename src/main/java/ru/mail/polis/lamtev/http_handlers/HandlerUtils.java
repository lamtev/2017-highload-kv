package ru.mail.polis.lamtev.http_handlers;

import com.sun.net.httpserver.HttpExchange;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public final class HandlerUtils {

    public static final String STATUS_PATH = "/v0/status";
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String INTERACTION_BETWEEN_NODES_PATH = "/v0/internal-connection";
    public static final String QUERY_PREFIX = "id=";
    public static final String STATUS_RESPONSE = "ONLINE";
    public static final String QUERY_IS_INVALID = "Query is invalid";
    public static final String VALUE_BY_ID = "Value by id=";
    public static final String MIGHT_HAVE_BEEN_DELETED = "might have been deleted";
    public static final String HAVE_BEEN_UPDATED = "have been updated";
    public static final String NOT_ALLOWED = "not allowed";
    public static final String GET = "GET";
    public static final String PUT = "PUT";
    public static final String DELETE = "DELETE";
    public static final String NOT_ENOUGH_REPLICAS = "Not enough replicas";
    public static final String NOT_FOUND = "Not found";
    public static final String ID = "?id=";
    public static final String CREATED = "Created";
    public static final String ILLEGAL_ID = "Illegal id";
    public static final String TOO_SMALL_RF = "Too small RF";
    public static final String TOO_BIG_RF = "Too big RF";
    protected static final String DELETED_ID = "Deleted id=";
    protected static final String DELETE_DELETED_ID_TRUE = "&deleteDeletedId=true";
    private static final String ID_PREFIX = "id=";
    private static final String REPLICAS_PREFIX = "&replicas=";
    private static final String DELETE_DELETED_IDS = "&deleteDeletedId=";

    public static void sendResponse(@NotNull HttpExchange http, @NotNull byte[] message, int code) throws IOException {
        http.sendResponseHeaders(code, message.length);
        http.getResponseBody().write(message);
        http.close();
    }

    public static void sendResponse(@NotNull HttpExchange http, @NotNull String message, int code) throws IOException {
        sendResponse(http, message.getBytes(), code);
    }

    @NotNull
    public static byte[] readData(@NotNull InputStream is) throws IOException {
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
    public static byte[] consistentValue(@NotNull List<byte[]> values, int ack) {
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
    public static QueryParams parseQuery(@NotNull String query) {
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

    public interface QueryParams {
        @NotNull
        String id();

        int ack();

        int from();

        boolean deleteDeletedId();
    }

}

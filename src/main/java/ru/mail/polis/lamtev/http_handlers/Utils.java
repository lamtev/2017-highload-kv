package ru.mail.polis.lamtev.http_handlers;

import com.sun.net.httpserver.HttpExchange;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public final class Utils {

    public static final String STATUS_PATH = "/v0/status";
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String INTERNAL_INTERACTION_PATH = "/v0/internal-connection";
    public static final String QUERY_PREFIX = "id=";
    public static final String STATUS_RESPONSE = "ONLINE";
    public static final String SHITTY_QUERY = "Shitty query";
    public static final String VALUE_BY_ID = "Value by id=";
    public static final String MIGHT_HAVE_BEEN_DELETED = "might have been deleted";
    public static final String HAVE_BEEN_UPDATED = "have been updated";
    public static final String NOT_ALLOWED = "not allowed";
    public static final String GET = "GET";
    public static final String PUT = "PUT";
    public static final String DELETE = "DELETE";

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
            byte[] buffer = new byte[1024];
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
            int cnt = (int) values.stream()
                    .filter(it -> Arrays.equals(it, value))
                    .count();
            if (cnt == ack) {
                return value;
            }
        }
        return values.get(0);
    }

    static final class QueryParser {

        private static final String ID_PREFIX = "id=";
        private static final String REPLICAS_PREFIX = "&replicas=";
        @NotNull
        private final String id;
        private final int ack;
        private final int from;

        QueryParser(@NotNull String query) {
            if (query.contains(REPLICAS_PREFIX)) {
                id = query.substring(ID_PREFIX.length(), query.indexOf(REPLICAS_PREFIX));
                final String[] replicas = query.substring(query.indexOf(REPLICAS_PREFIX) + REPLICAS_PREFIX.length()).split("/");
                final int mbAck = Integer.valueOf(replicas[0]);
                ack = mbAck > 0 ? mbAck : -1;
                final int mbFrom = Integer.valueOf(replicas[1]);
                from = mbFrom > 0 ? mbFrom : -1;
            } else {
                id = query.substring(ID_PREFIX.length());
                ack = from = 0;
            }
        }

        @NotNull
        public String id() {
            return id;
        }

        public int ack() {
            return ack;
        }

        public int from() {
            return from;
        }

        @Override
        public String toString() {
            return "QueryParser{" +
                    "id='" + id + '\'' +
                    ", ack=" + ack +
                    ", from=" + from +
                    '}';
        }
    }

}

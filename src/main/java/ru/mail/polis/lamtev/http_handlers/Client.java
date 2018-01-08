package ru.mail.polis.lamtev.http_handlers;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.fluent.Request;
import org.apache.http.impl.EnglishReasonPhraseCatalog;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;

import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.ID;
import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.INTERACTION_BETWEEN_NODES_PATH;

final class Client {

    static final BasicHttpResponse EXCEPTION_HTTP_RESPONSE = new BasicHttpResponse(
            new BasicStatusLine(
                    HttpVersion.HTTP_1_1,
                    500,
                    EnglishReasonPhraseCatalog.INSTANCE.getReason(500, Locale.ENGLISH)
            )
    );

    private Client() {
        throw new UnsupportedOperationException();
    }

    static HttpResponse sendGetRequest(@NotNull String node, @NotNull String id) {
        try {
            return Request.Get(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                    .execute()
                    .returnResponse();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static HttpResponse sendPutRequest(@NotNull String node, @NotNull String id, @NotNull byte[] data) {
        try {
            return Request.Put(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                    .bodyByteArray(data)
                    .execute()
                    .returnResponse();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static HttpResponse sendDeleteRequest(@NotNull String node, @NotNull String id) {
        try {
            return Request.Delete(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                    .execute()
                    .returnResponse();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}

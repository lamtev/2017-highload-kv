package ru.mail.polis.lamtev;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;

import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.ID;
import static ru.mail.polis.lamtev.http_handlers.HandlerUtils.INTERACTION_BETWEEN_NODES_PATH;

public class HttpClient implements Client {

    @Override
    public HttpResponse sendGetRequest(@NotNull String node, @NotNull String id) {
        try {
            return Request.Get(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                    .execute()
                    .returnResponse();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public HttpResponse sendPutRequest(@NotNull String node, @NotNull String id, @NotNull byte[] data) {
        try {
            return Request.Put(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                    .bodyByteArray(data)
                    .execute()
                    .returnResponse();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public HttpResponse sendDeleteRequest(@NotNull String node, @NotNull String id) {
        try {
            return Request.Delete(node + INTERACTION_BETWEEN_NODES_PATH + ID + id)
                    .execute()
                    .returnResponse();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}

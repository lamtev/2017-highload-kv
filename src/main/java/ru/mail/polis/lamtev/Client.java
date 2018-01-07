package ru.mail.polis.lamtev;

import org.apache.http.HttpResponse;
import org.jetbrains.annotations.NotNull;

public interface Client {
    HttpResponse sendGetRequest(@NotNull String node, @NotNull String id);

    HttpResponse sendPutRequest(@NotNull String node, @NotNull String id, @NotNull byte[] data);

    HttpResponse sendDeleteRequest(@NotNull String node, @NotNull String id);
}

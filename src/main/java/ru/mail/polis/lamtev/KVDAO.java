package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.NoSuchElementException;

public interface KVDAO {
    @NotNull
    byte[] get(@NotNull String id) throws NoSuchElementException, IOException;

    void upsert(@NotNull String id, @NotNull byte[] value) throws IOException;

    void delete(@NotNull String id) throws IOException;

    void deleteDeletedId(@NotNull String id) throws IOException;
}

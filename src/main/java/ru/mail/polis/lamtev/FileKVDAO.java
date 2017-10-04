package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

public class FileKVDAO implements KVDAO {

    @NotNull
    private final String dir;

    public FileKVDAO(@NotNull String dir) {
        this.dir = dir;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IllegalArgumentException, IOException {
        return Files.readAllBytes(Paths.get(dir, id));
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IllegalArgumentException, IOException {
        Files.write(Paths.get(dir, id), value);
    }

    @Override
    public void delete(@NotNull String id) throws IllegalArgumentException, IOException {
        Files.delete(Paths.get(dir, id));
    }
}

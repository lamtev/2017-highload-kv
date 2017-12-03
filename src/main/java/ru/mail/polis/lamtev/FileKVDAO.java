package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public final class FileKVDAO implements KVDAO {

    @NotNull
    private final String dir;
    @NotNull
    private final Set<String> deletedIds = new HashSet<>();

    public FileKVDAO(@NotNull String dir) {
        this.dir = dir;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkId(id);
        if (deletedIds.contains(id)) {
            throw new NoSuchElementException("DELETED");
        } else if (Files.notExists(Paths.get(dir, id))) {
            throw new NoSuchElementException("Can't find file with id=" + id);
        }
        return Files.readAllBytes(Paths.get(dir, id));
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IllegalArgumentException, IOException {
        checkId(id);
        Files.write(Paths.get(dir, id), value);
    }

    @Override
    public void delete(@NotNull String id) throws IllegalArgumentException, IOException {
        checkId(id);
        if (Files.deleteIfExists(Paths.get(dir, id))) {
            deletedIds.add(id);
        }
    }

    private void checkId(@NotNull String id) {
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Id is empty");
        }
    }

}

package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public final class FileKVDAO implements KVDAO {

    public static final String DELETED = "DELETED";
    private static final String CAN_T_FIND_FILE_WITH_ID = "Can't find file with id=";
    @NotNull
    private final String dir;
    @NotNull
    private final Set<String> deletedIds = new HashSet<>();

    public FileKVDAO(@NotNull String dir) {
        this.dir = dir;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IOException {
        if (deletedIds.contains(id)) {
            throw new NoSuchElementException(DELETED);
        } else if (Files.notExists(Paths.get(dir, id))) {
            throw new NoSuchElementException(CAN_T_FIND_FILE_WITH_ID + id);
        }
        return Files.readAllBytes(Paths.get(dir, id));
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IOException {
        Files.write(Paths.get(dir, id), value);
    }

    @Override
    public void delete(@NotNull String id) throws IOException {
        if (Files.deleteIfExists(Paths.get(dir, id))) {
            deletedIds.add(id);
        }
    }

}

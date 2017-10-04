package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

public class NioFileDAO implements KVDAO {

    @NotNull
    private final String dir;

    public NioFileDAO(@NotNull String dir) {
        this.dir = dir;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkId(id);
        if (Files.notExists(Paths.get(dir, id))) {
            throw new NoSuchElementException("Can't find file with id=" + id);
        }
        return Files.readAllBytes(Paths.get(dir, id));
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IllegalArgumentException, IOException {
        checkId(id);
        Path filePath = Paths.get(dir, id);
//        if (value.length == 0) {
//            Files.createFile(filePath);
//            return;
//        }
        Files.write(filePath, value);

    }

    @Override
    public void delete(@NotNull String id) throws IllegalArgumentException, IOException {
        checkId(id);
        Files.delete(Paths.get(dir, id));
    }

    private void checkId(@NotNull String id) {
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Id is empty");
        }
    }

}

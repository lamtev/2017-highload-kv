package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.NoSuchElementException;

public class OldGoodFileDAO implements KVDAO {

    @NotNull
    private final String dir;

    public OldGoodFileDAO(@NotNull String dir) {
        this.dir = dir;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IllegalArgumentException, IOException {
        final File file = getFile(id);
        if (!file.exists()) {
            throw new NoSuchElementException("Can't find file with id=" + id);
        }
        final byte[] value = new byte[(int) file.length()];
        try (InputStream in = new FileInputStream(file)) {
            if (in.read(value) != value.length) {
                throw new IOException("Can't read file in one go");
            }
        }
        return value;
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IllegalArgumentException, IOException {
        try (OutputStream out = new FileOutputStream(getFile(id))) {
            out.write(value);
        }
    }

    @Override
    public void delete(@NotNull String id) throws IllegalArgumentException, IOException {
        getFile(id).delete();
    }

    @NotNull
    private File getFile(@NotNull String id) {
        return new File(dir, id);
    }
}

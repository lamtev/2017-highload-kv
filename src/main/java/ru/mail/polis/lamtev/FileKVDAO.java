package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class FileKVDAO implements KVDAO {

    public static final String DELETED = "DELETED";
    private static final String CAN_T_FIND_FILE_WITH_ID = "Can't find file with id=";
    private static final String DELETED_IDS = "/deletedIds";
    @NotNull
    private final String dir;
    @NotNull
    private final String deletedIdsDir;
    @NotNull
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    @NotNull
    private final Lock readLock = readWriteLock.readLock();
    @NotNull
    private final Lock writeLock = readWriteLock.writeLock();

    public FileKVDAO(@NotNull String dir) {
        this.dir = dir;
        this.deletedIdsDir = dir + DELETED_IDS;
        createDeletedIdsDirectory();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IOException {
        readLock.lock();
        try {
            if (fileIsDeleted(id)) {
                throw new NoSuchElementException(DELETED);
            } else if (Files.notExists(Paths.get(dir, id))) {
                throw new NoSuchElementException(CAN_T_FIND_FILE_WITH_ID + id);
            }
            return Files.readAllBytes(Paths.get(dir, id));
        } finally {
            readLock.unlock();
        }

    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IOException {
        writeLock.lock();
        try {
            Files.write(Paths.get(dir, id), value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(@NotNull String id) throws IOException {
        writeLock.lock();
        try {
            if (Files.deleteIfExists(Paths.get(dir, id))) {
                Files.createFile(Paths.get(deletedIdsDir, id));
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void deleteDeletedId(@NotNull String id) throws IOException {
        writeLock.lock();
        try {
            Files.deleteIfExists(Paths.get(deletedIdsDir, id));
        } finally {
            writeLock.unlock();
        }
    }

    private void createDeletedIdsDirectory() {
        try {
            Files.createDirectory(Paths.get(deletedIdsDir));
        } catch (IOException ignored) {
            //Never happens
        }
    }

    private boolean fileIsDeleted(@NotNull String id) {
        return Files.exists(Paths.get(deletedIdsDir, id));
    }

}

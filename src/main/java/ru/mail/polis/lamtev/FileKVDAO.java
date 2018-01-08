package ru.mail.polis.lamtev;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.StampedLock;

public final class FileKVDAO implements KVDAO {

    public static final String DELETED = "DELETED";
    private static final String CAN_T_FIND_FILE_WITH_ID = "Can't find file with id=";
    private static final String DELETED_IDS = "/deletedIds";
    @NotNull
    private final String dir;
    @NotNull
    private final String deletedIdsDir;
    @NotNull
    private final StampedLock stampedLock = new StampedLock();

    public FileKVDAO(@NotNull String dir) {
        this.dir = dir;
        this.deletedIdsDir = dir + DELETED_IDS;
        createDeletedIdsDirectory();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull String id) throws NoSuchElementException, IOException {
        long stamp = stampedLock.tryOptimisticRead();
        checkFileExistence(id);
        byte[] data = Files.readAllBytes(Paths.get(dir, id));
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                checkFileExistence(id);
                data = Files.readAllBytes(Paths.get(dir, id));
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return data;
    }

    @Override
    public void upsert(@NotNull String id, @NotNull byte[] value) throws IOException {
        final long stamp = stampedLock.writeLock();
        try {
            Files.write(Paths.get(dir, id), value);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    @Override
    public void delete(@NotNull String id) throws IOException {
        final long stamp = stampedLock.writeLock();
        try {
            if (Files.deleteIfExists(Paths.get(dir, id))) {
                Files.createFile(Paths.get(deletedIdsDir, id));
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    @Override
    public void deleteDeletedId(@NotNull String id) throws IOException {
        final long stamp = stampedLock.writeLock();
        try {
            Files.deleteIfExists(Paths.get(deletedIdsDir, id));
        } finally {
            stampedLock.unlockWrite(stamp);
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

    private void checkFileExistence(@NotNull String id) {
        if (fileIsDeleted(id)) {
            throw new NoSuchElementException(DELETED);
        } else if (Files.notExists(Paths.get(dir, id))) {
            throw new NoSuchElementException(CAN_T_FIND_FILE_WITH_ID + id);
        }
    }

}

package ru.mail.polis.lamtev;

import java.util.NoSuchElementException;

public class FileHaveBeenDeletedException extends NoSuchElementException {

    private static final String PREFIX = "File with id=";
    private static final String POSTFIX = " have been deleted";

    public FileHaveBeenDeletedException(String id) {
        super(PREFIX + id + POSTFIX);
    }

}

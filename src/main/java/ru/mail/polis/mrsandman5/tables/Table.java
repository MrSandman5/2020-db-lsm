package ru.mail.polis.mrsandman5.tables;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.mrsandman5.models.Cell;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {

    long sizeInBytes();

    @NotNull
    Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException;

    void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException;

    void remove(@NotNull final ByteBuffer key) throws IOException;

}

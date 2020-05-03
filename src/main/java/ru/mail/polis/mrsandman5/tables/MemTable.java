package ru.mail.polis.mrsandman5.tables;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.mrsandman5.models.Cell;
import ru.mail.polis.mrsandman5.models.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public final class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes;

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> new Cell(e.getKey(), e.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final Value prev = map.put(key, Value.of(value));
        if (prev == null) {
            sizeInBytes += key.remaining() + value.remaining();
        } else if (prev.isTombstone()) {
            sizeInBytes += value.remaining();
        } else {
            sizeInBytes += value.remaining() - prev.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final Value prev = map.put(key, Value.tombstone());
        if (prev == null) {
            sizeInBytes += key.remaining();
        } else if (!prev.isTombstone()) {
            sizeInBytes -= prev.getData().remaining();
        }
    }

    @Override
    public void clear() {
        sizeInBytes = 0;
        map.clear();
    }

    public int getEntryCount() {
        return map.size();
    }
}

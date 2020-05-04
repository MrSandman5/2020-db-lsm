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

public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes;

    public MemTable(){
        sizeInBytes = 0;
    }

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
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        map.put(key.duplicate(), new Value(System.currentTimeMillis(), value.duplicate()));
        sizeInBytes += key.remaining() + value.remaining() + Long.BYTES;
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (map.containsKey(key)) {
            if (!map.get(key).isTombstone()) {
                sizeInBytes -= map.get(key).getData().remaining();
            }
        } else {
            sizeInBytes += key.remaining() + Long.BYTES;
        }
        map.put(key, new Value(System.currentTimeMillis()));
    }

    public int getEntryCount() {
        return map.size();
    }

}

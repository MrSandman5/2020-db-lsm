package ru.mail.polis.mrsandman5.tables;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.mrsandman5.models.Cell;
import ru.mail.polis.mrsandman5.models.Value;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SSTable implements Table{
    private final int rows;
    private final IntBuffer offsets;
    private final ByteBuffer cells;

    public SSTable(@NotNull final File file) throws IOException {
        final long fileSize = file.length();
        final ByteBuffer mapped;
        try(FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fileSize <= Integer.MAX_VALUE;
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fc.size()).order(ByteOrder.BIG_ENDIAN);
        }

        rows = mapped.getInt((int) (fileSize - Integer.BYTES));

        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(mapped.limit() - Integer.BYTES * rows - Integer.BYTES);
        offsetBuffer.limit(mapped.limit() - Integer.BYTES);
        this.offsets = offsetBuffer.slice().asIntBuffer();

        final ByteBuffer cellBuffer = mapped.duplicate();
        cellBuffer.limit(offsetBuffer.position());
        this.cells = cellBuffer.slice();
    }

    public static void write(final Iterator<Cell> cells, @NotNull final File file) throws IOException{
        try(FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (cells.hasNext()){
                offsets.add(offset);

                final Cell cell = cells.next();

                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();
                fc.write(fromInt(keySize));
                offset += Integer.BYTES;
                fc.write(key);
                offset += keySize;

                final Value value = cell.getValue();
                if (value.isTombstone()){
                    fc.write(fromLong(-cell.getValue().getTimestamp()));
                } else {
                    fc.write(fromLong(cell.getValue().getTimestamp()));
                }
                offset += Long.BYTES;

                if (!value.isTombstone()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fc.write(fromInt(valueSize));
                    offset += Integer.BYTES;
                    fc.write(valueData);
                    offset += valueSize;
                }
            }

            for (final Integer anOffset : offsets) {
                fc.write(fromInt(anOffset));
            }

            fc.write(fromInt(offsets.size()));
        }
    }

    private ByteBuffer keyAt(final int i){
        assert (0 <= i && i < rows);
        final int offset = offsets.get(i);
        final int keySize = cells.getInt(offset);
        final ByteBuffer key = cells.duplicate();
        key.position(offset + Integer.BYTES);
        key.limit(key.position() + keySize);
        return key.slice();
    }

    private Cell cellAt(final int i) {
        assert (0 <= i && i < rows);
        int offset = offsets.get(i);

        final int keySize = cells.getInt(offset);
        offset += Integer.BYTES;
        final ByteBuffer key = cells.duplicate();
        key.position(offset);
        key.limit(key.position() + keySize);
        offset += keySize;

        final long timestamp = cells.getLong(offset);
        offset += Long.BYTES;

        if (timestamp < 0) {
            return new Cell(key.slice(), new Value(-timestamp));
        } else {
            final int valueSize = cells.getInt(offset);
            offset += Integer.BYTES;
            final ByteBuffer value = cells.duplicate();
            value.position(offset);
            value.limit(value.position() + valueSize)
                    .position(offset)
                    .limit(offset + valueSize);
            return new Cell(key.slice(), new Value(timestamp, value.slice()));
        }
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = keyAt(mid).compareTo(from);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @Override
    public long sizeInBytes() {
        return 0;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {
            int next = position(from);
            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next++);
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException { }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException { }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("");
    }

    private static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        result.putInt(value);
        result.rewind();
        return result;
    }

    private static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        result.putLong(value);
        result.rewind();
        return result;
    }
}

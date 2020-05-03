package ru.mail.polis.mrsandman5;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;
import ru.mail.polis.mrsandman5.models.Cell;
import ru.mail.polis.mrsandman5.tables.MemTable;
import ru.mail.polis.mrsandman5.tables.SSTable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Stream;

public class DAOImpl implements DAO {

    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushThreshold;
    private final MemTable memTable;
    private final NavigableMap<Integer, SSTable> ssTables;
    private int generation = 0;

    public DAOImpl(@NotNull final File storage, final long flushThreshold) throws IOException {
        this.storage = storage;
        assert (flushThreshold >= 0L);
        this.flushThreshold = flushThreshold;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();
        try(Stream<Path> stream = Files.walk(storage.toPath(), 1)) {
            stream.filter(path -> path.getFileName().toString().endsWith(SUFFIX))
                    .forEach(path -> {
                        try {
                            final String name = path.getFileName().toString();
                            final int generation = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                            this.generation = Math.max(this.generation, generation);
                            ssTables.put(generation, new SSTable(path.toFile()));
                        } catch (IOException ignored){
                        }
                    });
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> fileIterators = new ArrayList<>(ssTables.size() + 1);
        fileIterators.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(v -> {
            try {
                fileIterators.add(v.iterator(from));
            } catch (IOException ex){
                throw new RuntimeException("Error", ex);
            }
        });

        final Iterator<Cell> cells = Iters.collapseEquals(Iterators.mergeSorted(fileIterators, Cell.COMPARATOR), Cell::getKey);
        final Iterator<Cell> fresh = Iterators.filter(
                cells, cell -> {
                    assert (cell != null);
                    return !cell.getValue().isTombstone();
                });
        return Iterators.transform(
                fresh, cell -> {
                    assert (cell != null);
                    return Record.of(cell.getKey(), cell.getValue().getData());
                }
        );
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() >= flushThreshold){
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() >= flushThreshold){
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.getEntryCount() > 0) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File temp = new File(storage, generation + TEMP);
        SSTable.write(memTable.iterator(ByteBuffer.allocate(0)), temp);
        final File file = new File(storage, generation + SUFFIX);
        Files.move(temp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables.put(generation, new SSTable(file));
        generation++;
        memTable.clear();
    }
}

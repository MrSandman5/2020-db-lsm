package ru.mail.polis.mrsandman5.models;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell implements Comparable<Cell> {

    //public static final Comparator<Cell> COMPARATOR = Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue);

    @NotNull
    private final ByteBuffer key;
    @NotNull
    private final Value value;

    public Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull Cell cell) {
        final int cmp = key.compareTo(cell.getKey());
        return cmp == 0 ? Long.compare(cell.getValue().getTimestamp(), value.getTimestamp()) : cmp;
    }
}

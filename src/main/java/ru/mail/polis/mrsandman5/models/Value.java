package ru.mail.polis.mrsandman5.models;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Optional;

public final class Value implements Comparable<Value> {

    private final long timestamp;
    @NotNull
    private final Optional<ByteBuffer> data;

    /**
     * Creates value from Bytebuffer.
     *
     * @param timestamp that represents time of creation
     * @param data buffer to get data to value
     */
    public Value(final long timestamp, @NotNull final ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = Optional.of(data);
    }

    /**
     * Creates empty value.
     *
     * @param timestamp that represents time of creation
     */
    public Value(final long timestamp) {
        this.timestamp = timestamp;
        this.data = Optional.empty();
    }

    public static Value of(final ByteBuffer data) {
        return new Value(System.currentTimeMillis(), data.duplicate());
    }

    public static Value tombstone() {
        return new Value(System.currentTimeMillis());
    }

    public boolean isTombstone() {
        return data.isEmpty();
    }

    public ByteBuffer getData() {
        return data.orElseThrow().asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }
}

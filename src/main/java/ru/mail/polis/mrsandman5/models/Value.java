package ru.mail.polis.mrsandman5.models;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Optional;

public final class Value implements Comparable<Value> {

    private final long timestamp;
    @NotNull
    private final Optional<ByteBuffer> data;

    public Value(final long timestamp, @NotNull final ByteBuffer data) {
        assert (timestamp > 0L);
        this.timestamp = timestamp;
        this.data = Optional.of(data);
    }

    public Value(final long timestamp) {
        assert (timestamp > 0L);
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
        assert (!isTombstone());
        return data.orElseThrow().asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }
}

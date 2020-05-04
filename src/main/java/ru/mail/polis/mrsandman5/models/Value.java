package ru.mail.polis.mrsandman5.models;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Optional;

public class Value {

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

    public boolean isTombstone() {
        return data.isEmpty();
    }

    public ByteBuffer getData() {
        return data.orElseThrow().asReadOnlyBuffer();
    }

    public long getTimestamp() {
        return timestamp;
    }
}

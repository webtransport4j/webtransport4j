package io.github.webtransport4j.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * A {@link BinarySource} backed by a {@link ReadableByteChannel}.
 * <p>
 * Calling {@link #close()} will close the underlying channel.
 */
final class ChannelBinarySource implements BinarySource {

    private final ReadableByteChannel channel;

    ChannelBinarySource(@NotNull ReadableByteChannel channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
    }

    @Override
    public int read(@NotNull ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
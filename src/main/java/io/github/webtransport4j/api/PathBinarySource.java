package io.github.webtransport4j.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.jspecify.annotations.NonNull;

/**
 * A {@link BinarySource} backed by a file {@link Path}.
 *
 * <p>This source efficiently determines the total file size and reads via a {@link
 * SeekableByteChannel}. Calling {@link #close()} closes the channel.
 */
final class PathBinarySource implements BinarySource {

  private final SeekableByteChannel channel;

  private final long size;

  PathBinarySource(Path path) throws IOException {
    this.channel = Files.newByteChannel(path, StandardOpenOption.READ);
    this.size = channel.size();
  }

  @Override
  public int read(@NonNull ByteBuffer dst) throws IOException {
    return channel.read(dst);
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public boolean hasKnownSize() {
    return true;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}

package io.github.webtransport4j.api;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.server.BinarySourceChunkedInput;
import io.github.webtransport4j.server.ByteBufBinarySource;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test cases for binary sources. */
public class BinarySourcesTest {

  private byte[] testData;
  private Path tempFile;

  /** Sets up test fixtures. */
  @BeforeEach
  public void setup() throws IOException {
    testData = "Hello WebTransport 4J".getBytes();
    tempFile = Files.createTempFile("wt4j", ".bin");
    Files.write(tempFile, testData);
  }

  @AfterEach
  public void teardown() throws IOException {
    Files.deleteIfExists(tempFile);
  }

  private void verifySource(BinarySource source) throws IOException {
    ByteBuffer dst = ByteBuffer.allocate(5);
    int totalRead = 0;

    while (true) {
      dst.clear();
      int read = source.read(dst);
      if (read == -1) {
        break;
      }
      dst.flip();

      // Verify read bytes
      for (int i = 0; i < read; i++) {
        assertEquals(testData[totalRead + i], dst.get());
      }

      totalRead += read;
    }

    assertEquals(testData.length, totalRead, "Total bytes read should match test data");
    source.close();
  }

  @Test
  public void testByteArrayBinarySource() throws IOException {
    BinarySource source = BinarySources.fromByteArray(testData);
    assertEquals(testData.length, source.size());
    assertTrue(source.hasKnownSize());
    verifySource(source);
  }

  @Test
  public void testByteArrayBinarySourceWithOffset() throws IOException {
    BinarySource source = BinarySources.fromByteArray(testData, 6, 12);
    assertEquals(12, source.size());

    ByteBuffer dst = ByteBuffer.allocate(20);
    int read = source.read(dst);
    assertEquals(12, read);
    dst.flip();

    byte[] expected = "WebTransport".getBytes();
    byte[] actual = new byte[12];
    dst.get(actual);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testByteBufferBinarySource() throws IOException {
    ByteBuffer buf = ByteBuffer.wrap(testData);
    BinarySource source = BinarySources.fromByteBuffer(buf);
    assertEquals(testData.length, source.size());
    verifySource(source);
    assertEquals(testData.length, buf.position(), "Underlying buffer position should be advanced");
  }

  @Test
  public void testByteBufBinarySource() throws IOException {
    ByteBuf buf = Unpooled.wrappedBuffer(testData);
    BinarySource source = new ByteBufBinarySource(buf);
    assertEquals(testData.length, source.size());
    verifySource(source);
    assertEquals(0, buf.readableBytes(), "Underlying Netty ByteBuf readerIndex should be exhausted");
  }

  @Test
  public void testChunkedInputUsesZeroCopyByteArrayChunks() throws Exception {
    byte[] data = "abcdef".getBytes();
    BinarySourceChunkedInput input =
        new BinarySourceChunkedInput(BinarySources.fromByteArray(data), 3);

    ByteBuf first = input.readChunk(ByteBufAllocator.DEFAULT);
    assertEquals(3, first.readableBytes());
    data[0] = 'z';
    assertEquals('z', first.getByte(0));
    first.release();

    ByteBuf second = input.readChunk(ByteBufAllocator.DEFAULT);
    assertEquals(3, second.readableBytes());
    second.release();

    assertNull(input.readChunk(ByteBufAllocator.DEFAULT));
    assertTrue(input.isEndOfInput());
  }

  @Test
  public void testChunkedInputUsesRetainedByteBufSlices() throws Exception {
    ByteBuf sourceBuffer = Unpooled.wrappedBuffer("abcdef".getBytes());
    BinarySourceChunkedInput input =
        new BinarySourceChunkedInput(new ByteBufBinarySource(sourceBuffer), 4);

    ByteBuf first = input.readChunk(ByteBufAllocator.DEFAULT);
    assertEquals(4, first.readableBytes());
    assertEquals(4, sourceBuffer.readerIndex());
    sourceBuffer.setByte(0, 'z');
    assertEquals('z', first.getByte(0));
    first.release();

    ByteBuf second = input.readChunk(ByteBufAllocator.DEFAULT);
    assertEquals(2, second.readableBytes());
    second.release();
    sourceBuffer.release();
  }

  @Test
  public void testPathBinarySource() throws IOException {
    BinarySource source = BinarySources.fromPath(tempFile);
    assertEquals(testData.length, source.size());
    verifySource(source);
  }

  @Test
  public void testFileBinarySource() throws IOException {
    File file = tempFile.toFile();
    BinarySource source = BinarySources.fromFile(file);
    assertEquals(testData.length, source.size());
    verifySource(source);
  }

  @Test
  public void testInputStreamBinarySource() throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(testData);
    BinarySource source = BinarySources.fromInputStream(in);
    assertEquals(-1, source.size(), "InputStream size is generally unknown");
    assertFalse(source.hasKnownSize());
    verifySource(source);
  }

  @Test
  public void testChannelBinarySource() throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(testData);
    ReadableByteChannel channel = Channels.newChannel(in);
    BinarySource source = BinarySources.fromReadableByteChannel(channel);
    assertEquals(-1, source.size());
    verifySource(source);
  }
}

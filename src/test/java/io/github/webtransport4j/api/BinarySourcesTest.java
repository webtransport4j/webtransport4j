package io.github.webtransport4j.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for binary sources. */
public class BinarySourcesTest {

  private byte[] testData;
  private Path tempFile;

  /** Sets up test fixtures. */
  @Before
  public void setup() throws IOException {
    testData = "Hello WebTransport 4J".getBytes();
    tempFile = Files.createTempFile("wt4j", ".bin");
    Files.write(tempFile, testData);
  }

  @After
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

    assertEquals("Total bytes read should match test data", testData.length, totalRead);
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
    assertEquals("Underlying buffer position should be advanced", testData.length, buf.position());
  }

  @Test
  public void testByteBufBinarySource() throws IOException {
    ByteBuf buf = Unpooled.wrappedBuffer(testData);
    BinarySource source = BinarySources.fromByteBuf(buf);
    assertEquals(testData.length, source.size());
    verifySource(source);
    assertEquals(
        "Underlying Netty ByteBuf readerIndex should be exhausted", 0, buf.readableBytes());
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
    assertEquals("InputStream size is generally unknown", -1, source.size());
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

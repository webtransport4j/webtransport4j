package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for memory segment binary source. */
public class MemorySegmentBinarySourceTest {

  private byte[] testData;

  @Before
  public void setup() {
    testData = "Hello Java 25 WebTransport!".getBytes(StandardCharsets.UTF_8);
  }

  @After
  public void teardown() {}

  @Test
  public void testMemorySegmentBinarySource() throws IOException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment segment = arena.allocateFrom(new String(testData, StandardCharsets.UTF_8));
      // Note: allocateFrom(String) writes a null terminator. We only care about the string bytes
      // for testing.
      // Let's allocate bytes instead.
      MemorySegment byteSegment = arena.allocate(testData.length);
      MemorySegment.copy(
          testData, 0, byteSegment, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, testData.length);

      BinarySource source = BinarySources.fromMemorySegment(byteSegment);

      assertEquals("Size should match", testData.length, source.size());

      ByteBuffer dst = ByteBuffer.allocate(10);
      int totalRead = 0;

      while (true) {
        dst.clear();
        int read = source.read(dst);
        if (read == -1) {
          break;
        }
        dst.flip();
        for (int i = 0; i < read; i++) {
          assertEquals("Bytes should match", testData[totalRead + i], dst.get());
        }
        totalRead += read;
      }

      assertEquals("Total bytes read should match", testData.length, totalRead);
      source.close();
    }
  }
}

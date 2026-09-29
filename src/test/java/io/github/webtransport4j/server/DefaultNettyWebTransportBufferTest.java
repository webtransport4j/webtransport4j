package io.github.webtransport4j.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/** Unit tests for {@link DefaultNettyWebTransportBuffer}. */
public class DefaultNettyWebTransportBufferTest {

  @Test
  public void testBufferRetainAndRelease() {
    ByteBuf byteBuf =
            Unpooled.buffer(16).writeBytes("test-payload".getBytes(StandardCharsets.UTF_8));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    try {
      assertEquals(1, buffer.refCnt());
      assertEquals(1, byteBuf.refCnt());

      WebTransportBuffer retained = buffer.retain();
      assertSame(buffer, retained);
      assertEquals(2, buffer.refCnt());
      assertEquals(1, byteBuf.refCnt());

      buffer.release();
      assertEquals(1, buffer.refCnt());
      assertEquals(1, byteBuf.refCnt());

      buffer.release();
      assertEquals(0, buffer.refCnt());
      assertEquals(0, byteBuf.refCnt());

      // Releasing an already released wrapper remains a no-op.
      buffer.release();
      buffer.close();
      assertEquals(0, buffer.refCnt());
      assertEquals(0, byteBuf.refCnt());
    } finally {
      releaseRemainingReferences(buffer);
    }
  }

  @Test
  public void testTryWithResourcesSafe() {
    ByteBuf byteBuf =
            Unpooled.buffer(16).writeBytes("try-with".getBytes(StandardCharsets.UTF_8));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    try (DefaultNettyWebTransportBuffer b = buffer) {
      assertEquals(8, b.readableBytes());
      assertEquals(1, b.refCnt());
      assertEquals(1, byteBuf.refCnt());
    }

    assertEquals(0, buffer.refCnt());
    assertEquals(0, byteBuf.refCnt());

    buffer.release();
    assertEquals(0, buffer.refCnt());
    assertEquals(0, byteBuf.refCnt());
  }

  @Test
  public void testCustomRetainIncrement() {
    ByteBuf byteBuf =
            Unpooled.buffer(16).writeBytes("increment".getBytes(StandardCharsets.UTF_8));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    try {
      buffer.retain(3);
      assertEquals(4, buffer.refCnt());
      assertEquals(1, byteBuf.refCnt());

      try {
        buffer.retain(0);
        fail("Expected IllegalArgumentException for zero increment");
      } catch (IllegalArgumentException expected) {
        assertEquals(4, buffer.refCnt());
        assertEquals(1, byteBuf.refCnt());
      }

      for (int remaining = 3; remaining > 0; remaining--) {
        buffer.release();
        assertEquals(remaining, buffer.refCnt());
        assertEquals(1, byteBuf.refCnt());
      }

      buffer.release();
      assertEquals(0, buffer.refCnt());
      assertEquals(0, byteBuf.refCnt());
    } finally {
      releaseRemainingReferences(buffer);
    }
  }

  @Test
  public void testRetainedReadableBuffer() {
    ByteBuf byteBuf =
            Unpooled.buffer(16).writeBytes("readable".getBytes(StandardCharsets.UTF_8));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);
    ByteBuf slice = null;

    try {
      slice = buffer.retainedReadableBuffer();

      // The slice owns an independent Netty reference.
      assertEquals(1, buffer.refCnt());
      assertEquals(2, byteBuf.refCnt());
      assertEquals("readable", slice.toString(StandardCharsets.UTF_8));

      slice.release();
      slice = null;
      assertEquals(1, byteBuf.refCnt());

      buffer.release();
      assertEquals(0, buffer.refCnt());
      assertEquals(0, byteBuf.refCnt());
    } finally {
      try {
        if (slice != null) {
          slice.release();
        }
      } finally {
        releaseRemainingReferences(buffer);
      }
    }
  }

  @Test
  public void testReadBytesAndSkipBytes() {
    byte[] src = "hello world".getBytes(StandardCharsets.UTF_8);
    ByteBuf byteBuf = Unpooled.copiedBuffer(src);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    try {
      assertEquals(src.length, buffer.readableBytes());

      ByteBuffer nio = buffer.nioBuffer();
      assertEquals(src.length, nio.remaining());

      buffer.skipBytes(6);
      assertEquals(5, buffer.readableBytes());

      byte[] remaining = buffer.readBytes();
      assertArrayEquals("world".getBytes(StandardCharsets.UTF_8), remaining);
      assertEquals(0, buffer.readableBytes());

      buffer.release();
      assertEquals(0, buffer.refCnt());
      assertEquals(0, byteBuf.refCnt());
    } finally {
      releaseRemainingReferences(buffer);
    }
  }

  private static void releaseRemainingReferences(DefaultNettyWebTransportBuffer buffer) {
    while (buffer.refCnt() > 0) {
      buffer.release();
    }
  }
}
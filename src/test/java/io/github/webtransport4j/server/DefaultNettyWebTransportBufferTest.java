package io.github.webtransport4j.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class DefaultNettyWebTransportBufferTest {

  @Test
  public void testBufferRetainAndRelease() {
    ByteBuf byteBuf = Unpooled.buffer(16).writeBytes("test-payload".getBytes(StandardCharsets.UTF_8));
    assertEquals(1, byteBuf.refCnt());

    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);
    assertEquals(1, buffer.refCnt());
    assertEquals(1, byteBuf.refCnt());

    WebTransportBuffer retained = buffer.retain();
    assertEquals(buffer, retained);
    assertEquals(2, buffer.refCnt());
    assertEquals(2, byteBuf.refCnt());

    buffer.release();
    assertEquals(1, buffer.refCnt());
    assertEquals(1, byteBuf.refCnt());

    buffer.release();
    assertEquals(0, buffer.refCnt());
    assertEquals(0, byteBuf.refCnt());

    // Safe double-release guard: Calling release on an already released buffer must be a no-op
    buffer.release();
    buffer.close();
    assertEquals(0, buffer.refCnt());
    assertEquals(0, byteBuf.refCnt());
  }

  @Test
  public void testTryWithResourcesSafe() {
    ByteBuf byteBuf = Unpooled.buffer(16).writeBytes("try-with".getBytes(StandardCharsets.UTF_8));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    try (DefaultNettyWebTransportBuffer b = buffer) {
      assertEquals(8, b.readableBytes());
      assertEquals(1, b.refCnt());
    }

    assertEquals(0, buffer.refCnt());
    assertEquals(0, byteBuf.refCnt());

    // Subsequent close/release in finally must not throw IllegalReferenceCountException
    buffer.release();
    assertEquals(0, buffer.refCnt());
  }

  @Test
  public void testCustomRetainIncrement() {
    ByteBuf byteBuf = Unpooled.buffer(16).writeBytes("increment".getBytes(StandardCharsets.UTF_8));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    buffer.retain(3);
    assertEquals(4, buffer.refCnt());
    assertEquals(4, byteBuf.refCnt());

    try {
      buffer.retain(0);
      fail("Expected IllegalArgumentException for zero increment");
    } catch (IllegalArgumentException expected) {
    }

    buffer.release();
    buffer.release();
    buffer.release();
    buffer.release();
    assertEquals(0, buffer.refCnt());
    assertEquals(0, byteBuf.refCnt());
  }

  @Test
  public void testRetainedReadableBuffer() {
    ByteBuf byteBuf = Unpooled.buffer(16).writeBytes("readable".getBytes(StandardCharsets.UTF_8));
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    ByteBuf slice = buffer.retainedReadableBuffer();
    assertEquals(2, byteBuf.refCnt());
    assertEquals("readable", slice.toString(StandardCharsets.UTF_8));

    slice.release();
    assertEquals(1, byteBuf.refCnt());

    buffer.release();
    assertEquals(0, byteBuf.refCnt());
  }

  @Test
  public void testReadBytesAndSkipBytes() {
    byte[] src = "hello world".getBytes(StandardCharsets.UTF_8);
    ByteBuf byteBuf = Unpooled.copiedBuffer(src);
    DefaultNettyWebTransportBuffer buffer = new DefaultNettyWebTransportBuffer(byteBuf);

    assertEquals(src.length, buffer.readableBytes());
    ByteBuffer nio = buffer.nioBuffer();
    assertEquals(src.length, nio.remaining());

    buffer.skipBytes(6);
    assertEquals(5, buffer.readableBytes());
    byte[] remaining = buffer.readBytes();
    assertArrayEquals("world".getBytes(StandardCharsets.UTF_8), remaining);

    buffer.release();
    assertEquals(0, byteBuf.refCnt());
  }
}

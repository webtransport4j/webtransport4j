package io.github.webtransport4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.server.DefaultNettyWebTransportBuffer;
import io.github.webtransport4j.server.FlyweightWebTransportBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Tests byte-level comparison and primitive access methods on WebTransport buffers without heap
 * allocations.
 */
public class ZeroAllocationBufferComparisonTest {

  private static final byte[] PING_CMD = "PING_12345678".getBytes(StandardCharsets.UTF_8);
  private static final byte[] PONG_CMD = "PONG_12345678".getBytes(StandardCharsets.UTF_8);
  private static final byte[] PREFIX_PING = "PING".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testFlyweightZeroAllocationComparisons() {
    ByteBuf direct = Unpooled.directBuffer(32);
    try {
      direct.writeBytes(PING_CMD);

      FlyweightWebTransportBuffer flyweight = FlyweightWebTransportBuffer.createFlyweight();
      flyweight.attach(direct);

      // Verify primitive byte reads
      assertEquals('P', (char) flyweight.getByte(0));
      assertEquals('I', (char) flyweight.getByte(1));
      assertEquals('N', (char) flyweight.getByte(2));
      assertEquals('G', (char) flyweight.getByte(3));

      // Verify equalsBytes without heap allocation
      assertTrue(flyweight.equalsBytes(PING_CMD));
      assertFalse(flyweight.equalsBytes(PONG_CMD));
      assertFalse(flyweight.equalsBytes(new byte[] {'P', 'I'}));

      // Verify startsWith without heap allocation
      assertTrue(flyweight.startsWith(PREFIX_PING));
      assertFalse(flyweight.startsWith(new byte[] {'P', 'O'}));

      // Verify readByte
      assertEquals('P', (char) flyweight.readByte());
      assertEquals(PING_CMD.length - 1, flyweight.readableBytes());
      assertEquals('I', (char) flyweight.getByte(0));

      flyweight.detach();
    } finally {
      direct.release();
    }
  }

  @Test
  public void testDefaultNettyBufferZeroAllocationComparisons() {
    ByteBuf direct = Unpooled.directBuffer(32);
    try {
      direct.writeInt(0x12345678);
      direct.writeLong(0x0102030405060708L);

      DefaultNettyWebTransportBuffer buf = DefaultNettyWebTransportBuffer.wrap(direct.retain());

      // Verify getInt and getLong without allocation
      assertEquals(0x12345678, buf.getInt(0));
      assertEquals(0x0102030405060708L, buf.getLong(4));

      assertTrue(buf.startsWith(new byte[] {0x12, 0x34}));
      assertFalse(buf.startsWith(new byte[] {0x12, 0x00}));

      buf.release();
    } finally {
      direct.release();
    }
  }
}

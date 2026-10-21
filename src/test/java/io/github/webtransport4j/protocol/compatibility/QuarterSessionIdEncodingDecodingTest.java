package io.github.webtransport4j.protocol.compatibility;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.webtransport4j.server.WebTransportDatagramDecoder;
import io.github.webtransport4j.server.WebTransportDatagramFrame;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Verifies Quarter Stream ID (Quarter Session ID) encoding and decoding per RFC and draft
 * specifications.
 *
 * <p>Specifications covered:
 *
 * <ul>
 *   <li>RFC 9000 Section 16: Variable-Length Integer Encoding.
 *   <li>RFC 9000 Section 2.1: Stream Types and Identifiers.
 *   <li>RFC 9297 Section 2: HTTP Datagram Format.
 *   <li>draft-ietf-webtrans-http3 Section 4.4 / 4.5: Datagrams and Quarter Stream ID mapping.
 * </ul>
 */
public class QuarterSessionIdEncodingDecodingTest {

  private static final byte[] TEST_PAYLOAD =
      "RFC_QUARTER_SESSION_ID_PAYLOAD".getBytes(StandardCharsets.UTF_8);

  /**
   * Tests RFC 9000 Section 16 1-byte VarInt encoding and decoding (values 0 to 63). Maps to Session
   * IDs 0 to 252 (quarterSessionId << 2).
   */
  @Test
  public void testOneByteVarIntQuarterSessionIdBoundaries() {
    // Boundary 0: Session ID = 0, Quarter = 0
    verifyVarIntQuarterSessionId(0L, 0L, 1, (byte) 0x00);

    // Quarter = 1, Session ID = 4
    verifyVarIntQuarterSessionId(4L, 1L, 1, (byte) 0x01);

    // Quarter = 37, Session ID = 148
    verifyVarIntQuarterSessionId(148L, 37L, 1, (byte) 0x25);

    // Boundary 63 (max 1-byte VarInt): Session ID = 252 (63 * 4), Quarter = 63
    verifyVarIntQuarterSessionId(252L, 63L, 1, (byte) 0x3F);
  }

  /**
   * Tests RFC 9000 Section 16 2-byte VarInt encoding and decoding (values 64 to 16,383). Maps to
   * Session IDs 256 to 65,532 (quarterSessionId << 2).
   */
  @Test
  public void testTwoByteVarIntQuarterSessionIdBoundaries() {
    // Boundary 64 (min 2-byte VarInt): Session ID = 256 (64 * 4), Quarter = 64
    // Encoded: 0x40 | (64 >> 8) = 0x40, (64 & 0xFF) = 0x40 -> [0x40, 0x40]
    verifyVarIntQuarterSessionId(256L, 64L, 2, (byte) 0x40);

    // Quarter = 1,000, Session ID = 4,000
    verifyVarIntQuarterSessionId(4000L, 1000L, 2, (byte) 0x43);

    // Boundary 16,383 (max 2-byte VarInt): Session ID = 65,532 (16,383 * 4), Quarter = 16,383
    // Encoded: 0x40 | (16383 >> 8) = 0x7F, 0xFF -> [0x7F, 0xFF]
    verifyVarIntQuarterSessionId(65532L, 16383L, 2, (byte) 0x7F);
  }

  /**
   * Tests RFC 9000 Section 16 4-byte VarInt encoding and decoding (values 16,384 to 1,073,741,823).
   * Maps to Session IDs 65,536 to 4,294,967,292 (quarterSessionId << 2).
   */
  @Test
  public void testFourByteVarIntQuarterSessionIdBoundaries() {
    // Boundary 16,384 (min 4-byte VarInt): Session ID = 65,536, Quarter = 16,384
    // Encoded: 0x80 | (16384 >> 24) = 0x80 -> [0x80, 0x00, 0x40, 0x00]
    verifyVarIntQuarterSessionId(65536L, 16384L, 4, (byte) 0x80);

    // Quarter = 1,000,000, Session ID = 4,000,000
    verifyVarIntQuarterSessionId(4000000L, 1000000L, 4, (byte) 0x80);

    // Boundary 1,073,741,823 (max 4-byte VarInt): Session ID = 4,294,967,292, Quarter =
    // 1,073,741,823
    // Encoded: 0x80 | 0x3F = 0xBF, 0xFF, 0xFF, 0xFF -> [0xBF, 0xFF, 0xFF, 0xFF]
    verifyVarIntQuarterSessionId(4294967292L, 1073741823L, 4, (byte) 0xBF);
  }

  /**
   * Tests RFC 9000 Section 16 8-byte VarInt encoding and decoding (values 1,073,741,824 to 2^62 -
   * 1). Maps to Session IDs 4,294,967,296 to (2^62 - 4).
   */
  @Test
  public void testEightByteVarIntQuarterSessionIdBoundaries() {
    // Boundary 1,073,741,824 (min 8-byte VarInt): Session ID = 4,294,967,296, Quarter =
    // 1,073,741,824
    // Encoded: 0xC0 | (1073741824 >> 56) = 0xC0 -> [0xC0, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00]
    verifyVarIntQuarterSessionId(4294967296L, 1073741824L, 8, (byte) 0xC0);

    // Arbitrary large Quarter ID: 10,000,000,000L, Session ID = 40,000,000,000L
    verifyVarIntQuarterSessionId(40000000000L, 10000000000L, 8, (byte) 0xC0);

    // Max 62-bit VarInt value allowed by QUIC RFC 9000: 0x3FFFFFFFFFFFFFFFL
    // For Session ID: must satisfy (quarterSessionId << 2) <= 0x3FFFFFFFFFFFFFFFL,
    // so quarterSessionId = 0x3FFFFFFFFFFFFFFFL >> 2 = 0x0FFFFFFFFFFFFFFFL
    long maxQuarter = 0x3FFFFFFFFFFFFFFFL >> 2;
    long maxSessionId = maxQuarter << 2;
    verifyVarIntQuarterSessionId(maxSessionId, maxQuarter, 8, (byte) 0xC0);
  }

  /**
   * Tests RFC 9000 Section 2.1 client-initiated bidirectional stream validation. WebTransport
   * CONNECT streams must have streamId % 4 == 0.
   */
  @Test
  public void testClientInitiatedBidirectionalStreamIdClassification() {
    // Valid WebTransport CONNECT stream IDs (Client-Initiated Bidirectional: 0x00)
    long[] validClientBidi = {0L, 4L, 8L, 12L, 16L, 256L, 1024L, 65536L, 4294967296L};
    for (long id : validClientBidi) {
      assertTrue(WebTransportUtils.isClientInitiatedBidirectionalStream(id), "Stream ID " + id + " must be client-initiated bidirectional");
      assertEquals(id, (id >> 2) << 2, "Quarter ID must reconstruct exact Session ID when shifted by 2");
    }

    // Invalid: Server-Initiated Bidirectional (0x01: streamId % 4 == 1)
    long[] serverBidi = {1L, 5L, 9L, 13L, 257L};
    for (long id : serverBidi) {
      assertFalse(WebTransportUtils.isClientInitiatedBidirectionalStream(id), "Server-initiated bidi stream " + id + " must NOT be valid WebTransport CONNECT session");
    }

    // Invalid: Client-Initiated Unidirectional (0x02: streamId % 4 == 2)
    long[] clientUni = {2L, 6L, 10L, 14L, 258L};
    for (long id : clientUni) {
      assertFalse(WebTransportUtils.isClientInitiatedBidirectionalStream(id), "Client uni stream " + id + " must NOT be valid WebTransport CONNECT session");
    }

    // Invalid: Server-Initiated Unidirectional (0x03: streamId % 4 == 3)
    long[] serverUni = {3L, 7L, 11L, 15L, 259L};
    for (long id : serverUni) {
      assertFalse(WebTransportUtils.isClientInitiatedBidirectionalStream(id), "Server uni stream " + id + " must NOT be valid WebTransport CONNECT session");
    }

    // Invalid: Negative stream IDs
    assertFalse(WebTransportUtils.isClientInitiatedBidirectionalStream(-1L));
    assertFalse(WebTransportUtils.isClientInitiatedBidirectionalStream(-4L));
  }

  /** Tests WebTransportDatagramDecoder behavior when VarInt bytes are fragmented / truncated. */
  @Test
  public void testDecoderHandlingOfFragmentedVarIntBytes() {
    EmbeddedChannel channel = new EmbeddedChannel(WebTransportDatagramDecoder.INSTANCE);

    // 1. Empty buffer returns -1 and produces 0 frames
    ByteBuf emptyBuf = Unpooled.buffer();
    channel.writeInbound(emptyBuf);
    assertNull(channel.readInbound(), "Empty buffer must produce no datagram frame");

    // 2. Incomplete 2-byte VarInt (first byte is 0x40 -> declares 2 bytes, but only 1 byte given)
    ByteBuf partial2Byte = Unpooled.buffer();
    partial2Byte.writeByte(0x40);
    channel.writeInbound(partial2Byte);
    assertNull(channel.readInbound(), "Incomplete 2-byte VarInt must produce no frame");

    // 3. Incomplete 4-byte VarInt (first byte is 0x80 -> declares 4 bytes, only 2 bytes given)
    ByteBuf partial4Byte = Unpooled.buffer();
    partial4Byte.writeByte(0x80);
    partial4Byte.writeByte(0x00);
    channel.writeInbound(partial4Byte);
    assertNull(channel.readInbound(), "Incomplete 4-byte VarInt must produce no frame");

    // 4. Incomplete 8-byte VarInt (first byte is 0xC0 -> declares 8 bytes, only 3 bytes given)
    ByteBuf partial8Byte = Unpooled.buffer();
    partial8Byte.writeByte(0xC0);
    partial8Byte.writeByte(0x00);
    partial8Byte.writeByte(0x00);
    channel.writeInbound(partial8Byte);
    assertNull(channel.readInbound(), "Incomplete 8-byte VarInt must produce no frame");

    channel.finishAndReleaseAll();
  }

  /** Tests out-of-range VarInt values (> 62 bits or negative) throw IllegalArgumentException. */
  @Test
  public void testOutOfRangeVarIntValidation() {
    ByteBuf buf = Unpooled.buffer();

    // Negative value
    try {
      WebTransportUtils.writeVarInt(buf, -1L);
      fail("writeVarInt must reject negative values");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    // Negative value for varIntLength
    try {
      WebTransportUtils.varIntLength(-1L);
      fail("varIntLength must reject negative values");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    // Overflow value (> 2^62 - 1 = 0x3FFFFFFFFFFFFFFFL)
    long overflow = 0x4000000000000000L;
    try {
      WebTransportUtils.writeVarInt(buf, overflow);
      fail("writeVarInt must reject values > 62 bits");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      WebTransportUtils.varIntLength(overflow);
      fail("varIntLength must reject values > 62 bits");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    buf.release();
  }

  /**
   * Helper that encodes a Quarter Session ID and validates length, prefix bits, and full decode
   * roundtrip through WebTransportDatagramDecoder.
   */
  private static void verifyVarIntQuarterSessionId(
      long sessionId, long quarterSessionId, int expectedLength, byte expectedPrefixByte) {

    // 1. Validate quarter session ID calculation
    assertEquals(quarterSessionId, sessionId >> 2, "Quarter session ID must be sessionId >> 2");
    assertEquals(sessionId, quarterSessionId << 2, "Session ID must reconstruct to quarterSessionId << 2");

    // 2. Validate VarInt length calculation
    int calculatedLen = WebTransportUtils.varIntLength(quarterSessionId);
    assertEquals(expectedLength, calculatedLen, "Calculated VarInt length must match expected length");

    // 3. Write VarInt + payload to buffer
    ByteBuf buf = Unpooled.buffer();
    WebTransportUtils.writeVarInt(buf, quarterSessionId);
    buf.writeBytes(TEST_PAYLOAD);

    assertEquals(expectedLength, buf.readableBytes() - TEST_PAYLOAD.length, "Encoded VarInt bytes must match expected length");

    // 4. Verify 2MSB length prefix encoding in the first byte
    byte firstByte = buf.getByte(0);
    int prefixBits = (firstByte & 0xFF) >> 6;
    int expectedPrefixBits;
    switch (expectedLength) {
      case 1:
        expectedPrefixBits = 0; // 0b00
        break;
      case 2:
        expectedPrefixBits = 1; // 0b01
        break;
      case 4:
        expectedPrefixBits = 2; // 0b10
        break;
      case 8:
        expectedPrefixBits = 3; // 0b11
        break;
      default:
        throw new IllegalArgumentException("Unexpected length: " + expectedLength);
    }
    assertEquals(expectedPrefixBits, prefixBits, "2MSB prefix bits must match VarInt length power");

    // 5. Test raw readVariableLengthInt
    ByteBuf readCheckBuf = buf.duplicate();
    long decodedQuarter = WebTransportUtils.readVariableLengthInt(readCheckBuf);
    assertEquals(quarterSessionId, decodedQuarter, "readVariableLengthInt must recover exact quarterSessionId");
    assertEquals(TEST_PAYLOAD.length, readCheckBuf.readableBytes(), "readVariableLengthInt must consume exactly expected VarInt bytes");

    // 6. Test full pipeline decode through WebTransportDatagramDecoder
    EmbeddedChannel channel = new EmbeddedChannel(WebTransportDatagramDecoder.INSTANCE);
    channel.writeInbound(buf);

    WebTransportDatagramFrame frame = channel.readInbound();
    assertNotNull(frame, "Decoder must output WebTransportDatagramFrame");
    assertEquals(sessionId, frame.sessionId(), "Frame session ID must exactly match original session ID");

    byte[] payloadBytes = new byte[frame.content().readableBytes()];
    frame.content().readBytes(payloadBytes);
    frame.release();
    assertArrayEquals(TEST_PAYLOAD, payloadBytes, "Decoded datagram payload must match original payload");

    channel.finishAndReleaseAll();
  }
}

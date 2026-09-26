package io.github.webtransport4j.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.net.ssl.SSLEngine;
import org.junit.Test;

/**
 * Tests verifying the TLS Keying Material Exporter context structure and domain separation
 * per draft-ietf-webtrans-http3-16 § 4.8.
 */
public class WebTransportKeyExporterTest {

  @Test
  public void testSerializeExporterContextRfcFigure6Layout() {
    long sessionId = 4L;
    String appLabel = "custom-auth";
    byte[] appCtx = "challenge-xyz".getBytes(StandardCharsets.UTF_8);

    byte[] serialized = WebTransportKeyExporter.serializeExporterContext(sessionId, appLabel, appCtx);

    // 8 (sessionId) + 1 (labelLen) + 11 (label) + 1 (ctxLen) + 13 (ctx) = 34 bytes
    assertEquals(34, serialized.length);

    ByteBuf buf = Unpooled.wrappedBuffer(serialized);
    try {
      // 1. WebTransport Session ID (64)
      assertEquals(4L, buf.readLong());
      // 2. WebTransport Application-Supplied Exporter Label Length (8)
      int labelLen = buf.readUnsignedByte();
      assertEquals(11, labelLen);
      // 3. WebTransport Application-Supplied Exporter Label (8..)
      byte[] labelBytes = new byte[labelLen];
      buf.readBytes(labelBytes);
      assertEquals("custom-auth", new String(labelBytes, StandardCharsets.US_ASCII));
      // 4. WebTransport Application-Supplied Exporter Context Length (8)
      int ctxLen = buf.readUnsignedByte();
      assertEquals(13, ctxLen);
      // 5. WebTransport Application-Supplied Exporter Context (..)
      byte[] ctxBytes = new byte[ctxLen];
      buf.readBytes(ctxBytes);
      assertArrayEquals(appCtx, ctxBytes);
      assertFalse(buf.isReadable());
    } finally {
      buf.release();
    }
  }

  @Test
  public void testSerializeExporterContext_OmittedContext() {
    long sessionId = 0L;
    String appLabel = "test";

    byte[] serialized = WebTransportKeyExporter.serializeExporterContext(sessionId, appLabel, null);

    // 8 (sessionId) + 1 (labelLen) + 4 (label) + 1 (ctxLen) + 0 (ctx) = 14 bytes
    assertEquals(14, serialized.length);

    ByteBuf buf = Unpooled.wrappedBuffer(serialized);
    try {
      assertEquals(0L, buf.readLong());
      assertEquals(4, buf.readUnsignedByte());
      byte[] labelBytes = new byte[4];
      buf.readBytes(labelBytes);
      assertEquals("test", new String(labelBytes, StandardCharsets.US_ASCII));
      // Context length must be 0
      assertEquals(0, buf.readUnsignedByte());
      assertFalse(buf.isReadable());
    } finally {
      buf.release();
    }
  }

  @Test
  public void testDomainSeparationBetweenSessions() {
    // When two distinct sessions on the same QUIC connection request keys using identical
    // label and context, the derived keys MUST be different due to different Session IDs in context.
    byte[] fakeMasterSecret = new byte[32];
    Arrays.fill(fakeMasterSecret, (byte) 0x42);

    byte[] ctxSession0 = WebTransportKeyExporter.serializeExporterContext(0L, "app-key", new byte[] {1, 2, 3});
    byte[] ctxSession4 = WebTransportKeyExporter.serializeExporterContext(4L, "app-key", new byte[] {1, 2, 3});

    byte[] keySession0 = WebTransportKeyExporter.deriveKeyingMaterialHkdf(
        fakeMasterSecret, WebTransportKeyExporter.TLS_EXPORTER_LABEL, ctxSession0, 32);
    byte[] keySession4 = WebTransportKeyExporter.deriveKeyingMaterialHkdf(
        fakeMasterSecret, WebTransportKeyExporter.TLS_EXPORTER_LABEL, ctxSession4, 32);

    assertEquals(32, keySession0.length);
    assertEquals(32, keySession4.length);
    assertFalse("Keys for different sessions MUST be cryptographically distinct",
        Arrays.equals(keySession0, keySession4));
  }

  @Test
  public void testValidationBounds() {
    assertThrows(NullPointerException.class, () ->
        WebTransportKeyExporter.serializeExporterContext(0L, null, null));

    // Label > 255 bytes
    char[] longChars = new char[256];
    Arrays.fill(longChars, 'x');
    assertThrows(IllegalArgumentException.class, () ->
        WebTransportKeyExporter.serializeExporterContext(0L, new String(longChars), null));

    // Context > 255 bytes
    byte[] longCtx = new byte[256];
    assertThrows(IllegalArgumentException.class, () ->
        WebTransportKeyExporter.serializeExporterContext(0L, "ok", longCtx));

    assertThrows(IllegalArgumentException.class, () ->
        WebTransportKeyExporter.exportKeyingMaterial(null, "ok", new byte[0], 0));
  }

  @Test
  public void testWebTransportSessionExportKeyingMaterialIntegration() {
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);
    when(mockStream.parent()).thenReturn(mockQuic);
    SSLEngine mockEngine = mock(SSLEngine.class);
    when(mockQuic.sslEngine()).thenReturn(mockEngine);

    WebTransportSession session = new WebTransportSession(
        100L, mockStream, "/test", 10L, 10L, 1000L, 10L, 10L, 1000L, true, true);

    // Underlying Mockito SSLEngine does not implement exportKeyingMaterial, so expect UnsupportedOperationException
    UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class, () ->
        session.exportKeyingMaterial("custom-auth", new byte[] {1, 2}, 32));
    assertNotNull(ex.getMessage());
  }
}

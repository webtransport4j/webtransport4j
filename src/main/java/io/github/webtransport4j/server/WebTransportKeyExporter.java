package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.quic.QuicChannel;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLEngine;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Implements TLS Keying Material Exporter support for WebTransport over HTTP/3
 * as per draft-ietf-webtrans-http3-16 Section 4.8 and RFC 8446 Section 7.5.
 */
public final class WebTransportKeyExporter {

  /**
   * The fixed TLS 1.3 exporter label required by draft-16 § 4.8.
   */
  public static final String TLS_EXPORTER_LABEL = "EXPORTER-WebTransport";

  private WebTransportKeyExporter() {}

  /**
   * Serializes the "WebTransport Exporter Context" struct as defined in draft-16 Figure 6.
   * <pre>
   * WebTransport Exporter Context {
   *   WebTransport Session ID (64),
   *   WebTransport Application-Supplied Exporter Label Length (8),
   *   WebTransport Application-Supplied Exporter Label (8..),
   *   WebTransport Application-Supplied Exporter Context Length (8),
   *   WebTransport Application-Supplied Exporter Context (..)
   * }
   * </pre>
   *
   * @param sessionId the 64-bit WebTransport session ID
   * @param appLabel the application-supplied exporter label
   * @param appCtx the optional application-supplied exporter context (null = zero-length)
   * @return byte array containing the serialized context struct
   */
  public static byte[] serializeExporterContext(
      long sessionId,
      @NonNull String appLabel,
      byte @Nullable [] appCtx) {
    Objects.requireNonNull(appLabel, "appLabel must not be null");
    byte[] labelBytes = appLabel.getBytes(StandardCharsets.US_ASCII);
    if (labelBytes.length > 255) {
      throw new IllegalArgumentException(
          "appLabel length exceeds 255 bytes (was " + labelBytes.length + ")");
    }
    byte[] ctxBytes = (appCtx != null) ? appCtx : new byte[0];
    if (ctxBytes.length > 255) {
      throw new IllegalArgumentException(
          "appCtx length exceeds 255 bytes (was " + ctxBytes.length + ")");
    }

    ByteBuf buf = Unpooled.buffer(8 + 1 + labelBytes.length + 1 + ctxBytes.length);
    try {
      // 1. WebTransport Session ID (64-bit integer, Big-Endian)
      buf.writeLong(sessionId);
      // 2. WebTransport Application-Supplied Exporter Label Length (8-bit)
      buf.writeByte(labelBytes.length);
      // 3. WebTransport Application-Supplied Exporter Label (variable bytes)
      buf.writeBytes(labelBytes);
      // 4. WebTransport Application-Supplied Exporter Context Length (8-bit)
      buf.writeByte(ctxBytes.length);
      // 5. WebTransport Application-Supplied Exporter Context (variable bytes)
      if (ctxBytes.length > 0) {
        buf.writeBytes(ctxBytes);
      }
      byte[] out = new byte[buf.readableBytes()];
      buf.readBytes(out);
      return out;
    } finally {
      buf.release();
    }
  }

  /**
   * Invokes the TLS Keying Material Exporter on the underlying QUIC SSLEngine.
   *
   * @param quic the QUIC channel
   * @param label the exporter label (MUST be "EXPORTER-WebTransport")
   * @param context the serialized WebTransport Exporter Context
   * @param length the desired length of exported keying material in bytes
   * @return the exported keying material
   */
  public static byte[] exportKeyingMaterial(
      @Nullable QuicChannel quic,
      @NonNull String label,
      byte @NonNull [] context,
      int length) {
    if (length <= 0) {
      throw new IllegalArgumentException("Key length must be positive: " + length);
    }
    if (quic == null) {
      throw new IllegalStateException("QUIC channel is not connected or available");
    }
    SSLEngine sslEngine = quic.sslEngine();
    if (sslEngine == null) {
      throw new IllegalStateException("SSLEngine is not available on QUIC channel");
    }

    // Attempt reflection for TLS exporter methods (supported by Conscrypt / BoringSSL / Java 21+ previews)
    try {
      Method exportMethod = sslEngine.getClass().getMethod(
          "exportKeyingMaterial", String.class, byte[].class, int.class);
      return (byte[]) exportMethod.invoke(sslEngine, label, context, length);
    } catch (NoSuchMethodException ignored) {
      // Check alternative method signature: exportKeyingMaterial(label, context, contextOffset, contextLen, length)
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke TLS Keying Material Exporter", e);
    }

    throw new UnsupportedOperationException(
        "TLS Keying Material Exporter is not supported by the underlying QUIC TLS engine ("
            + sslEngine.getClass().getName()
            + "). Please configure a TLS engine with exportKeyingMaterial support.");
  }

  /**
   * HKDF-Expand key derivation helper for testing and offline cryptographic verification (RFC 5869).
   */
  public static byte[] deriveKeyingMaterialHkdf(
      byte @NonNull [] prk,
      @NonNull String label,
      byte @NonNull [] context,
      int length) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(prk, "HmacSHA256"));
      byte[] labelBytes = label.getBytes(StandardCharsets.US_ASCII);
      ByteBuf info = Unpooled.buffer(labelBytes.length + context.length);
      info.writeBytes(labelBytes);
      info.writeBytes(context);
      byte[] infoBytes = new byte[info.readableBytes()];
      info.readBytes(infoBytes);
      info.release();

      byte[] result = new byte[length];
      byte[] previousT = new byte[0];
      int generated = 0;
      byte counter = 1;

      while (generated < length) {
        mac.reset();
        mac.update(previousT);
        mac.update(infoBytes);
        mac.update(counter);
        previousT = mac.doFinal();
        int toCopy = Math.min(previousT.length, length - generated);
        System.arraycopy(previousT, 0, result, generated, toCopy);
        generated += toCopy;
        counter++;
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException("HKDF derivation failed", e);
    }
  }
}

package io.github.webtransport4j.server;

import io.github.webtransport4j.cluster.StatelessTokenSecretProvider;
import io.github.webtransport4j.cluster.StaticTokenSecretProvider;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicTokenHandler;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cryptographically secure implementation of {@link QuicTokenHandler} using HMAC-SHA256. It
 * mitigates QUIC connection source address spoofing/amplification attacks by signing and verifying
 * the client's IP address, token timestamp, and destination connection ID (dcid).
 */
public class HmacQuicTokenHandler implements QuicTokenHandler {

  private static final Logger logger = LoggerFactory.getLogger(HmacQuicTokenHandler.class);

  private static final String HMAC_ALGO = "HmacSHA256";

  // HMAC-SHA256 signature length (256 bits)
  private static final int SIGNATURE_LEN = 32;

  // long timestamp length (64 bits)
  private static final int TIMESTAMP_LEN = 8;

  // 40 bytes verification header
  private static final int TOKEN_LEN = TIMESTAMP_LEN + SIGNATURE_LEN;

  // 60 seconds
  private static final long DEFAULT_EXPIRATION_MS = 60_000L;

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private static final class ThreadLocalState {
    final Mac mac;
    final byte[] timestampBytes = new byte[TIMESTAMP_LEN];
    final byte[] signatureBytes = new byte[SIGNATURE_LEN];
    final byte[] expectedSignatureBytes = new byte[SIGNATURE_LEN];
    final byte[] scratch = new byte[256];

    ThreadLocalState() {
      try {
        this.mac = Mac.getInstance(HMAC_ALGO);
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("Algorithm unavailable: " + HMAC_ALGO, e);
      }
    }
  }

  private static final ThreadLocal<ThreadLocalState> THREAD_LOCAL_STATE =
      ThreadLocal.withInitial(ThreadLocalState::new);

  private final StatelessTokenSecretProvider secretProvider;

  private final long expirationMs;

  public HmacQuicTokenHandler() {
    this(generateRandomKey(), DEFAULT_EXPIRATION_MS);
  }

  public HmacQuicTokenHandler(long expirationMs) {
    this(generateRandomKey(), expirationMs);
  }

  /**
   * Constructs an HMAC token handler with a static key.
   *
   * @param key static HMAC secret key (at least 16 bytes)
   * @param expirationMs token validity window in milliseconds
   */
  public HmacQuicTokenHandler(byte[] key, long expirationMs) {
    this(new StaticTokenSecretProvider(key), expirationMs);
  }

  /**
   * Constructs an HMAC token handler backed by a {@link StatelessTokenSecretProvider}.
   *
   * @param secretProvider provider supplying active and rotated validation keys across cluster
   * @param expirationMs token validity window in milliseconds
   */
  public HmacQuicTokenHandler(
      @NonNull StatelessTokenSecretProvider secretProvider, long expirationMs) {
    Objects.requireNonNull(secretProvider, "secretProvider must not be null");
    if (expirationMs <= 0) {
      throw new IllegalArgumentException("expirationMs must be positive: " + expirationMs);
    }
    this.secretProvider = secretProvider;
    this.expirationMs = expirationMs;
  }

  static byte @NonNull [] generateRandomKey() {
    byte[] key = new byte[32];
    SECURE_RANDOM.nextBytes(key);
    return key;
  }

  @Override
  public boolean writeToken(
      @NonNull ByteBuf out, @NonNull ByteBuf dcid, @NonNull InetSocketAddress address) {
    final long timestamp = System.currentTimeMillis();
    final ThreadLocalState state = THREAD_LOCAL_STATE.get();
    try {
      final SecretKeySpec keySpec = secretProvider.getActiveSecretKeySpec();
      final Mac mac = state.mac;
      mac.init(keySpec);

      // Update with timestamp (big-endian)
      for (int i = 0; i < 8; i++) {
        state.timestampBytes[i] = (byte) (timestamp >>> (56 - i * 8));
      }
      mac.update(state.timestampBytes, 0, 8);

      // Update with IP bytes
      if (address.getAddress() != null) {
        mac.update(address.getAddress().getAddress());
      } else {
        mac.update(address.getHostString().getBytes(StandardCharsets.UTF_8));
      }

      // Update with the connection ID bytes
      int dcidLen = dcid.readableBytes();
      if (dcidLen > 0) {
        if (dcidLen <= state.scratch.length) {
          dcid.getBytes(dcid.readerIndex(), state.scratch, 0, dcidLen);
          mac.update(state.scratch, 0, dcidLen);
        } else {
          byte[] temp = new byte[dcidLen];
          dcid.getBytes(dcid.readerIndex(), temp);
          mac.update(temp);
        }
      }

      mac.doFinal(state.signatureBytes, 0);

      // Write token header
      out.writeLong(timestamp);
      out.writeBytes(state.signatureBytes, 0, SIGNATURE_LEN);
      // Append the original connection ID to the token so the server can recover it on validation
      out.writeBytes(dcid, dcid.readerIndex(), dcidLen);
      return true;
    } catch (Exception e) {
      logger.error("Failed to generate HMAC token", e);
      return false;
    }
  }

  @Override
  public int validateToken(@NonNull ByteBuf token, @NonNull InetSocketAddress address) {
    if (token.readableBytes() < TOKEN_LEN) {
      return -1;
    }
    final long timestamp = token.readLong();
    final ThreadLocalState state = THREAD_LOCAL_STATE.get();
    token.readBytes(state.signatureBytes, 0, SIGNATURE_LEN);
    final long now = System.currentTimeMillis();
    // Validate timestamp (prevent replay and future timestamp anomalies)
    if (now - timestamp > expirationMs || timestamp - now > expirationMs) {
      return -1;
    }
    try {
      final List<SecretKeySpec> validSpecs = secretProvider.getValidationSecretKeySpecs();
      for (int i = 0; i < 8; i++) {
        state.timestampBytes[i] = (byte) (timestamp >>> (56 - i * 8));
      }
      byte[] ipBytes =
          (address.getAddress() != null)
              ? address.getAddress().getAddress()
              : address.getHostString().getBytes(StandardCharsets.UTF_8);
      int dcidLen = token.readableBytes();
      if (dcidLen <= state.scratch.length) {
        token.getBytes(token.readerIndex(), state.scratch, 0, dcidLen);
      }
      for (SecretKeySpec keySpec : validSpecs) {
        Mac mac = state.mac;
        mac.init(keySpec);
        mac.update(state.timestampBytes, 0, 8);
        mac.update(ipBytes);
        // The remaining bytes in the token represent the destination connection ID
        if (dcidLen <= state.scratch.length) {
          mac.update(state.scratch, 0, dcidLen);
        } else {
          byte[] temp = new byte[dcidLen];
          token.getBytes(token.readerIndex(), temp);
          mac.update(temp);
        }
        mac.doFinal(state.expectedSignatureBytes, 0);
        if (MessageDigest.isEqual(state.signatureBytes, state.expectedSignatureBytes)) {
          // Return the start offset of the destination connection ID (dcid) in the token buffer
          return token.readerIndex();
        }
      }
    } catch (Exception e) {
      logger.error("Failed to validate HMAC token", e);
    }
    return -1;
  }

  @Override
  public int maxTokenLength() {
    // 40 bytes validation header + support up to 216 bytes for connection ID
    return 256;
  }
}

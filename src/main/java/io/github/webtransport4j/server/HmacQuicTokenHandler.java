package io.github.webtransport4j.server;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.quic.QuicTokenHandler;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.SecureRandom;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * A cryptographically secure implementation of {@link QuicTokenHandler} using HMAC-SHA256.
 * It mitigates QUIC connection source address spoofing/amplification attacks by signing
 * and verifying the client's IP address, token timestamp, and destination connection ID (dcid).
 */
public class HmacQuicTokenHandler implements QuicTokenHandler {
  private static final Logger logger = LoggerFactory.getLogger(HmacQuicTokenHandler.class);
  private static final String HMAC_ALGO = "HmacSHA256";
  private static final int SIGNATURE_LEN = 32; // HMAC-SHA256 signature length (256 bits)
  private static final int TIMESTAMP_LEN = 8;  // long timestamp length (64 bits)
  private static final int TOKEN_LEN = TIMESTAMP_LEN + SIGNATURE_LEN; // 40 bytes verification header
  private static final long DEFAULT_EXPIRATION_MS = 60_000L; // 60 seconds
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private final SecretKeySpec keySpec;
  private final long expirationMs;

  public HmacQuicTokenHandler() {
    this(generateRandomKey(), DEFAULT_EXPIRATION_MS);
  }

  public HmacQuicTokenHandler(long expirationMs) {
    this(generateRandomKey(), expirationMs);
  }

  public HmacQuicTokenHandler(byte[] key, long expirationMs) {
    if (key == null || key.length < 16) {
      throw new IllegalArgumentException("Key must be at least 16 bytes");
    }
    this.keySpec = new SecretKeySpec(key, HMAC_ALGO);
    this.expirationMs = expirationMs;
  }

  static byte[] generateRandomKey() {
    byte[] key = new byte[32];
    SECURE_RANDOM.nextBytes(key);
    return key;
  }

  @Override
  public boolean writeToken(ByteBuf out, ByteBuf dcid, InetSocketAddress address) {
    long timestamp = System.currentTimeMillis();
    byte[] ipBytes = address.getAddress().getAddress();
    
    try {
      Mac mac = Mac.getInstance(HMAC_ALGO);
      mac.init(keySpec);
      
      // Update with timestamp (big-endian)
      byte[] timestampBytes = new byte[8];
      for (int i = 0; i < 8; i++) {
        timestampBytes[i] = (byte) (timestamp >>> (56 - i * 8));
      }
      mac.update(timestampBytes);
      
      // Update with IP bytes
      mac.update(ipBytes);
      
      // Update with the connection ID bytes
      mac.update(dcid.nioBuffer(dcid.readerIndex(), dcid.readableBytes()));
      
      byte[] signature = mac.doFinal();
      
      // Write token header
      out.writeLong(timestamp);
      out.writeBytes(signature);
      
      // Append the original connection ID to the token so the server can recover it on validation
      out.writeBytes(dcid, dcid.readerIndex(), dcid.readableBytes());
      return true;
    } catch (Exception e) {
      logger.error("Failed to generate HMAC token", e);
      return false;
    }
  }

  @Override
  public int validateToken(ByteBuf token, InetSocketAddress address) {
    if (token.readableBytes() < TOKEN_LEN) {
      return -1;
    }
    
    long timestamp = token.readLong();
    byte[] signature = new byte[SIGNATURE_LEN];
    token.readBytes(signature);
    
    long now = System.currentTimeMillis();
    // Validate timestamp (prevent replay and future timestamp anomalies)
    if (now - timestamp > expirationMs || timestamp - now > expirationMs) {
      return -1;
    }
    
    byte[] ipBytes = address.getAddress().getAddress();
    try {
      Mac mac = Mac.getInstance(HMAC_ALGO);
      mac.init(keySpec);
      
      byte[] timestampBytes = new byte[8];
      for (int i = 0; i < 8; i++) {
        timestampBytes[i] = (byte) (timestamp >>> (56 - i * 8));
      }
      mac.update(timestampBytes);
      mac.update(ipBytes);
      
      // The remaining bytes in the token represent the destination connection ID
      mac.update(token.nioBuffer(token.readerIndex(), token.readableBytes()));
      
      byte[] expectedSignature = mac.doFinal();
      
      if (MessageDigest.isEqual(signature, expectedSignature)) {
        // Return the start offset of the destination connection ID (dcid) in the token buffer
        return token.readerIndex();
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

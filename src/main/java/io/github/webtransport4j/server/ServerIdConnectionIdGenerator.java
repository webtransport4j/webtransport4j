package io.github.webtransport4j.server;

import io.netty.handler.codec.quic.QuicConnectionIdGenerator;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A {@link QuicConnectionIdGenerator} that embeds a Server ID into the generated Destination
 * Connection IDs (DCIDs), enabling L4 load balancers (e.g., Maglev, Katran, IPVS, eBPF/XDP)
 * to route QUIC packets to the correct server node based on Connection ID routing
 * (IETF draft-ietf-quic-load-balancers / RFC 9000).
 *
 * <p>Format of generated Connection IDs:
 * <pre>
 * +------------------------+------------------------------------+
 * |  Server ID (1-4 bytes) | Cryptographic Random Entropy Bytes |
 * +------------------------+------------------------------------+
 * </pre>
 *
 * <p>When a client roams or performs active connection migration (e.g., from Wi-Fi to Cellular),
 * its 4-tuple IP/port changes. Traditional L4 hash-based balancers would send the packet to a
 * different server. With Server ID embedded in the Connection ID, the L4 balancer extracts the
 * Server ID prefix and forwards the packet directly to the correct backend node without state tables.
 */
public class ServerIdConnectionIdGenerator implements QuicConnectionIdGenerator {

  public static final int DEFAULT_MAX_CONN_ID_LEN = 20;

  private final byte[] serverIdBytes;
  private final SecureRandom random = new SecureRandom();

  /**
   * Creates a generator with a single-byte server ID (0 to 255).
   *
   * @param serverId the unique server ID within the cluster (0-255)
   */
  public ServerIdConnectionIdGenerator(int serverId) {
    if (serverId < 0 || serverId > 255) {
      throw new IllegalArgumentException(
          "Single-byte serverId must be in range [0, 255], got: " + serverId);
    }
    this.serverIdBytes = new byte[] {(byte) (serverId & 0xFF)};
  }

  /**
   * Creates a generator with an explicit byte array server ID (1 to 8 bytes).
   *
   * @param serverIdBytes the server ID bytes
   */
  public ServerIdConnectionIdGenerator(byte @NonNull [] serverIdBytes) {
    if (serverIdBytes == null || serverIdBytes.length == 0 || serverIdBytes.length > 8) {
      throw new IllegalArgumentException("serverIdBytes must be between 1 and 8 bytes long");
    }
    this.serverIdBytes = serverIdBytes.clone();
  }

  @Override
  public @NonNull ByteBuffer newId(int length) {
    if (length < serverIdBytes.length) {
      throw new IllegalArgumentException(
          "Requested Connection ID length ("
              + length
              + ") is smaller than server ID length ("
              + serverIdBytes.length
              + ")");
    }
    byte[] cid = new byte[length];
    System.arraycopy(serverIdBytes, 0, cid, 0, serverIdBytes.length);

    int entropyLen = length - serverIdBytes.length;
    if (entropyLen > 0) {
      byte[] entropy = new byte[entropyLen];
      random.nextBytes(entropy);
      System.arraycopy(entropy, 0, cid, serverIdBytes.length, entropyLen);
    }
    return ByteBuffer.wrap(cid);
  }

  @Override
  public @NonNull ByteBuffer newId(@Nullable ByteBuffer buffer, int length) {
    return newId(length);
  }

  @Override
  public int maxConnectionIdLength() {
    return DEFAULT_MAX_CONN_ID_LEN;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  /**
   * Returns a copy of the server ID bytes embedded in all generated Connection IDs.
   *
   * @return byte array containing the server ID
   */
  public byte[] getServerIdBytes() {
    return serverIdBytes.clone();
  }

  @Override
  public String toString() {
    return "ServerIdConnectionIdGenerator{" + "serverIdBytes=" + Arrays.toString(serverIdBytes) + '}';
  }
}

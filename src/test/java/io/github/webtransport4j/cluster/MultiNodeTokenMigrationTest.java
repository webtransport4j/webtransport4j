package io.github.webtransport4j.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.server.HmacQuicTokenHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import org.junit.Test;

/**
 * Tests for multi-node QUIC token exchange and zero-downtime key rotation across cluster nodes.
 */
public class MultiNodeTokenMigrationTest {

  @Test
  public void testCrossNodeTokenValidation() throws Exception {
    final byte[] sharedSecret = new byte[32];
    Arrays.fill(sharedSecret, (byte) 77);

    final StatelessTokenSecretProvider clusterProvider = new StaticTokenSecretProvider(sharedSecret);

    final HmacQuicTokenHandler node1Handler = new HmacQuicTokenHandler(clusterProvider, 30_000L);
    final HmacQuicTokenHandler node2Handler = new HmacQuicTokenHandler(clusterProvider, 30_000L);

    final ByteBuf tokenBuf = Unpooled.buffer();
    final ByteBuf dcid = Unpooled.buffer();
    dcid.writeLong(987654321L);

    final InetSocketAddress clientAddress =
        new InetSocketAddress(InetAddress.getByName("192.168.1.100"), 54321);

    // Node 1 writes token for client
    assertTrue(node1Handler.writeToken(tokenBuf, dcid, clientAddress));

    // Node 2 validates token from client arriving at Node 2
    final int offset = node2Handler.validateToken(tokenBuf, clientAddress);
    assertEquals(40, offset);

    tokenBuf.release();
    dcid.release();
  }

  @Test
  public void testZeroDowntimeSecretRotationValidation() throws Exception {
    final byte[] secretV1 = new byte[32];
    Arrays.fill(secretV1, (byte) 1);
    final byte[] secretV2 = new byte[32];
    Arrays.fill(secretV2, (byte) 2);
    final byte[] secretV3 = new byte[32];
    Arrays.fill(secretV3, (byte) 3);

    // Retention: 1 previous secret
    final RotatingTokenSecretProvider node1Secrets = new RotatingTokenSecretProvider(secretV1, 1);
    final RotatingTokenSecretProvider node2Secrets = new RotatingTokenSecretProvider(secretV1, 1);

    final HmacQuicTokenHandler node1Handler = new HmacQuicTokenHandler(node1Secrets, 30_000L);
    final HmacQuicTokenHandler node2Handler = new HmacQuicTokenHandler(node2Secrets, 30_000L);

    final ByteBuf tokenV1 = Unpooled.buffer();
    final ByteBuf dcid = Unpooled.buffer();
    dcid.writeLong(11111L);
    final InetSocketAddress clientAddress =
        new InetSocketAddress(InetAddress.getByName("10.0.0.1"), 12345);

    // Issue token under V1
    assertTrue(node1Handler.writeToken(tokenV1, dcid, clientAddress));

    // Rotate cluster to V2
    node1Secrets.rotateSecret(secretV2);
    node2Secrets.rotateSecret(secretV2);

    // Token issued under V1 MUST still validate on Node 2 using historical fallback
    final int offsetV1 = node2Handler.validateToken(tokenV1.duplicate(), clientAddress);
    assertEquals(40, offsetV1);

    // New token issued under V2 MUST validate on Node 2 using active secret
    final ByteBuf tokenV2 = Unpooled.buffer();
    assertTrue(node1Handler.writeToken(tokenV2, dcid, clientAddress));
    final int offsetV2 = node2Handler.validateToken(tokenV2, clientAddress);
    assertEquals(40, offsetV2);

    // Rotate cluster to V3 (evicting V1 since retention=1)
    node1Secrets.rotateSecret(secretV3);
    node2Secrets.rotateSecret(secretV3);

    // Now token issued under V1 MUST be rejected (-1)
    final int rejectedOffset = node2Handler.validateToken(tokenV1, clientAddress);
    assertEquals(-1, rejectedOffset);

    tokenV1.release();
    tokenV2.release();
    dcid.release();
  }
}

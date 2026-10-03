package io.github.webtransport4j.server;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link ServerIdConnectionIdGenerator}. */
public class ServerIdConnectionIdGeneratorTest {

  @Test
  public void testSingleByteServerIdGeneration() {
    int serverId = 42;
    ServerIdConnectionIdGenerator generator = new ServerIdConnectionIdGenerator(serverId);

    Assert.assertArrayEquals(new byte[] {42}, generator.getServerIdBytes());
    Assert.assertEquals(20, generator.maxConnectionIdLength());
    Assert.assertFalse(generator.isIdempotent());

    ByteBuffer cidBuf = generator.newId(16);
    Assert.assertNotNull(cidBuf);
    Assert.assertEquals(16, cidBuf.remaining());

    byte[] cid = new byte[16];
    cidBuf.get(cid);

    // Verify first byte is serverId
    Assert.assertEquals((byte) 42, cid[0]);

    // Verify subsequent IDs are unique (entropy present)
    ByteBuffer cidBuf2 = generator.newId(16);
    byte[] cid2 = new byte[16];
    cidBuf2.get(cid2);
    Assert.assertEquals((byte) 42, cid2[0]);
    Assert.assertFalse("Consecutive CIDs should not be identical", Arrays.equals(cid, cid2));
  }

  @Test
  public void testMultiByteServerIdGeneration() {
    byte[] serverId = new byte[] {0x01, 0x02, (byte) 0xFE};
    ServerIdConnectionIdGenerator generator = new ServerIdConnectionIdGenerator(serverId);

    Assert.assertArrayEquals(serverId, generator.getServerIdBytes());

    ByteBuffer cidBuf = generator.newId(null, 12);
    byte[] cid = new byte[12];
    cidBuf.get(cid);

    Assert.assertEquals((byte) 0x01, cid[0]);
    Assert.assertEquals((byte) 0x02, cid[1]);
    Assert.assertEquals((byte) 0xFE, cid[2]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSingleByteServerIdNegative() {
    new ServerIdConnectionIdGenerator(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSingleByteServerIdOver255() {
    new ServerIdConnectionIdGenerator(256);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyByteArrayServerId() {
    new ServerIdConnectionIdGenerator(new byte[0]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequestedLengthSmallerThanServerId() {
    ServerIdConnectionIdGenerator generator =
        new ServerIdConnectionIdGenerator(new byte[] {1, 2, 3, 4});
    generator.newId(2);
  }

  @Test
  public void testServerBuilderWithServerId() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(8443)
            .serverId(5)
            .build();

    Assert.assertNotNull(server);
    Assert.assertFalse(server.isStarted());
  }
}

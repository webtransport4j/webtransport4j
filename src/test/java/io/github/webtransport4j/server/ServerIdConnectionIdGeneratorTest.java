package io.github.webtransport4j.server;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link ServerIdConnectionIdGenerator}. */
public class ServerIdConnectionIdGeneratorTest {

  @Test
  public void testSingleByteServerIdGeneration() {
    int serverId = 42;
    ServerIdConnectionIdGenerator generator = new ServerIdConnectionIdGenerator(serverId);

    Assertions.assertArrayEquals(new byte[] {42}, generator.getServerIdBytes());
    Assertions.assertEquals(20, generator.maxConnectionIdLength());
    Assertions.assertFalse(generator.isIdempotent());

    ByteBuffer cidBuf = generator.newId(16);
    Assertions.assertNotNull(cidBuf);
    Assertions.assertEquals(16, cidBuf.remaining());

    byte[] cid = new byte[16];
    cidBuf.get(cid);

    // Verify first byte is serverId
    Assertions.assertEquals((byte) 42, cid[0]);

    // Verify subsequent IDs are unique (entropy present)
    ByteBuffer cidBuf2 = generator.newId(16);
    byte[] cid2 = new byte[16];
    cidBuf2.get(cid2);
    Assertions.assertEquals((byte) 42, cid2[0]);
    Assertions.assertFalse(Arrays.equals(cid, cid2), "Consecutive CIDs should not be identical");
  }

  @Test
  public void testMultiByteServerIdGeneration() {
    byte[] serverId = new byte[] {0x01, 0x02, (byte) 0xFE};
    ServerIdConnectionIdGenerator generator = new ServerIdConnectionIdGenerator(serverId);

    Assertions.assertArrayEquals(serverId, generator.getServerIdBytes());

    ByteBuffer cidBuf = generator.newId(null, 12);
    byte[] cid = new byte[12];
    cidBuf.get(cid);

    Assertions.assertEquals((byte) 0x01, cid[0]);
    Assertions.assertEquals((byte) 0x02, cid[1]);
    Assertions.assertEquals((byte) 0xFE, cid[2]);
  }

  @Test
  public void testInvalidSingleByteServerIdNegative() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ServerIdConnectionIdGenerator(-1);

    });
  }

  @Test
  public void testInvalidSingleByteServerIdOver255() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ServerIdConnectionIdGenerator(256);

    });
  }

  @Test
  public void testEmptyByteArrayServerId() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new ServerIdConnectionIdGenerator(new byte[0]);

    });
  }

  @Test
  public void testRequestedLengthSmallerThanServerId() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      ServerIdConnectionIdGenerator generator =
          new ServerIdConnectionIdGenerator(new byte[] {1, 2, 3, 4});
      generator.newId(2);

    });
  }

  @Test
  public void testServerBuilderWithServerId() {
    WebTransportServer server = WebTransportServer.builder().port(8443).serverId(5).build();

    Assertions.assertNotNull(server);
    Assertions.assertFalse(server.isStarted());
  }
}

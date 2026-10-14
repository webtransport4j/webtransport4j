package io.github.webtransport4j.cluster;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.netty.channel.DefaultEventLoop;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Tests for {@link StaticTokenSecretProvider} and {@link RotatingTokenSecretProvider}.
 */
public class StatelessTokenSecretProviderTest {

  @Test
  public void testStaticTokenSecretProvider() {
    final byte[] key1 = new byte[32];
    Arrays.fill(key1, (byte) 1);
    final byte[] key2 = new byte[32];
    Arrays.fill(key2, (byte) 2);

    final StaticTokenSecretProvider provider =
        new StaticTokenSecretProvider(key1, Collections.singletonList(key2));

    assertArrayEquals(key1, provider.getActiveSecret());
    final List<byte[]> valid = provider.getValidationSecrets();
    assertEquals(2, valid.size());
    assertArrayEquals(key1, valid.get(0));
    assertArrayEquals(key2, valid.get(1));
    valid.get(0)[0] = 9;
    valid.get(1)[0] = 9;
    assertArrayEquals(key1, provider.getValidationSecrets().get(0));
    assertArrayEquals(key2, provider.getValidationSecrets().get(1));
    try {
      valid.clear();
      fail("validation secrets must be unmodifiable");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }

  @Test
  public void testRotatingTokenSecretProviderRetention() {
    final byte[] keyA = new byte[32];
    Arrays.fill(keyA, (byte) 'A');
    final byte[] keyB = new byte[32];
    Arrays.fill(keyB, (byte) 'B');
    final byte[] keyC = new byte[32];
    Arrays.fill(keyC, (byte) 'C');
    final byte[] keyD = new byte[32];
    Arrays.fill(keyD, (byte) 'D');

    // Max historical = 2
    final RotatingTokenSecretProvider provider = new RotatingTokenSecretProvider(keyA, 2);
    assertArrayEquals(keyA, provider.getActiveSecret());
    assertEquals(1, provider.getValidationSecrets().size());

    // Rotate to B
    provider.rotateSecret(keyB);
    assertArrayEquals(keyB, provider.getActiveSecret());
    List<byte[]> valid = provider.getValidationSecrets();
    assertEquals(2, valid.size());
    assertArrayEquals(keyB, valid.get(0));
    assertArrayEquals(keyA, valid.get(1));

    // Rotate to C
    provider.rotateSecret(keyC);
    assertArrayEquals(keyC, provider.getActiveSecret());
    valid = provider.getValidationSecrets();
    assertEquals(3, valid.size()); // C, B, A

    // Rotate to D -> A should be evicted (max 2 history: D active + C, B)
    provider.rotateSecret(keyD);
    assertArrayEquals(keyD, provider.getActiveSecret());
    valid = provider.getValidationSecrets();
    assertEquals(3, valid.size());
    assertArrayEquals(keyD, valid.get(0));
    assertArrayEquals(keyC, valid.get(1));
    assertArrayEquals(keyB, valid.get(2));
  }

  @Test(timeout = 5000)
  public void testRotatingTokenSecretProviderEventLoopSafety() throws Exception {
    final byte[] keyA = new byte[32];
    Arrays.fill(keyA, (byte) 'A');
    final byte[] keyB = new byte[32];
    Arrays.fill(keyB, (byte) 'B');

    final RotatingTokenSecretProvider provider = new RotatingTokenSecretProvider(keyA, 2);
    final DefaultEventLoop loop = new DefaultEventLoop();
    try {
      loop.submit(
              () -> {
                // Must be allowed and non-blocking on event loop thread
                assertArrayEquals(keyA, provider.getActiveSecret());
                assertEquals(1, provider.getValidationSecrets().size());
                assertArrayEquals(keyA, provider.getValidationSecrets().get(0));

                // rotateSecret is a control-plane mutation and must be rejected on event loop
                assertThrows(IllegalStateException.class, () -> provider.rotateSecret(keyB));
              })
          .get(3, TimeUnit.SECONDS);

      // Verify rotation works outside event loop
      provider.rotateSecret(keyB);
      loop.submit(
              () -> {
                assertArrayEquals(keyB, provider.getActiveSecret());
                assertEquals(2, provider.getValidationSecrets().size());
              })
          .get(3, TimeUnit.SECONDS);
    } finally {
      loop.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
    }
  }
}

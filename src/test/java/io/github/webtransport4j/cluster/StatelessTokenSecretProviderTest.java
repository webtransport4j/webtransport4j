package io.github.webtransport4j.cluster;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
}

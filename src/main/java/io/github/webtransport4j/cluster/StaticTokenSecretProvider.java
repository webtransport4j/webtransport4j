package io.github.webtransport4j.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.NonNull;

/**
 * Immutable {@link StatelessTokenSecretProvider} implementation holding static cluster secrets.
 */
public class StaticTokenSecretProvider implements StatelessTokenSecretProvider {

  private final byte[] activeSecret;
  private final List<byte[]> validationSecrets;

  /**
   * Constructs a provider with a single active secret.
   *
   * @param secret primary shared secret (at least 16 bytes)
   */
  public StaticTokenSecretProvider(byte @NonNull [] secret) {
    this(secret, Collections.emptyList());
  }

  /**
   * Constructs a provider with a primary secret and fallback verification secrets.
   *
   * @param activeSecret primary secret used for signing
   * @param fallbackSecrets historical secrets permitted for verification
   */
  public StaticTokenSecretProvider(byte @NonNull [] activeSecret, @NonNull List<byte[]> fallbackSecrets) {
    Objects.requireNonNull(activeSecret, "activeSecret must not be null");
    Objects.requireNonNull(fallbackSecrets, "fallbackSecrets must not be null");
    if (activeSecret.length < 16) {
      throw new IllegalArgumentException("Secret must be at least 16 bytes, was: " + activeSecret.length);
    }
    this.activeSecret = Arrays.copyOf(activeSecret, activeSecret.length);

    final List<byte[]> list = new ArrayList<>(1 + fallbackSecrets.size());
    list.add(this.activeSecret);
    for (byte[] fb : fallbackSecrets) {
      if (fb != null && fb.length >= 16) {
        list.add(Arrays.copyOf(fb, fb.length));
      }
    }
    this.validationSecrets = Collections.unmodifiableList(list);
  }

  @Override
  public byte @NonNull [] getActiveSecret() {
    return Arrays.copyOf(activeSecret, activeSecret.length);
  }

  @Override
  public @NonNull List<byte[]> getValidationSecrets() {
    return validationSecrets;
  }
}

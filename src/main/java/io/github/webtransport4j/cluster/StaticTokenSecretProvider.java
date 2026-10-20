package io.github.webtransport4j.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.crypto.spec.SecretKeySpec;
import org.jspecify.annotations.NonNull;

/**
 * Immutable {@link StatelessTokenSecretProvider} implementation holding static cluster secrets.
 */
public class StaticTokenSecretProvider implements StatelessTokenSecretProvider {

  private final byte[] activeSecret;
  private final List<byte[]> validationSecrets;
  private final SecretKeySpec activeSecretKeySpec;
  private final List<SecretKeySpec> validationSecretKeySpecs;

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
    this.activeSecretKeySpec = new SecretKeySpec(this.activeSecret, "HmacSHA256");

    final List<byte[]> list = new ArrayList<>(1 + fallbackSecrets.size());
    final List<SecretKeySpec> specList = new ArrayList<>(1 + fallbackSecrets.size());
    list.add(this.activeSecret);
    specList.add(this.activeSecretKeySpec);
    for (byte[] fb : fallbackSecrets) {
      if (fb != null && fb.length >= 16) {
        byte[] copy = Arrays.copyOf(fb, fb.length);
        list.add(copy);
        specList.add(new SecretKeySpec(copy, "HmacSHA256"));
      }
    }
    this.validationSecrets = Collections.unmodifiableList(list);
    this.validationSecretKeySpecs = Collections.unmodifiableList(specList);
  }

  @Override
  public byte @NonNull [] getActiveSecret() {
    return Arrays.copyOf(activeSecret, activeSecret.length);
  }

  @Override
  public @NonNull List<byte[]> getValidationSecrets() {
    final List<byte[]> copies = new ArrayList<>(validationSecrets.size());
    for (byte[] secret : validationSecrets) {
      copies.add(Arrays.copyOf(secret, secret.length));
    }
    return Collections.unmodifiableList(copies);
  }

  @Override
  public @NonNull SecretKeySpec getActiveSecretKeySpec() {
    return activeSecretKeySpec;
  }

  @Override
  public @NonNull List<SecretKeySpec> getValidationSecretKeySpecs() {
    return validationSecretKeySpecs;
  }
}

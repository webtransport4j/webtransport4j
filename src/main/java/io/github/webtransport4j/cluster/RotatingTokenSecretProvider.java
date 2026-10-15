package io.github.webtransport4j.cluster;

import io.github.webtransport4j.internal.EventLoopSafety;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;

/**
 * Thread-safe {@link StatelessTokenSecretProvider} supporting runtime key rotation without downtime.
 */
public class RotatingTokenSecretProvider implements StatelessTokenSecretProvider {

  private static final class SecretSnapshot {
    final byte[] activeSecret;
    final List<byte[]> validationSecrets;

    SecretSnapshot(byte[] activeSecret, Deque<byte[]> historicalSecrets) {
      this.activeSecret = Arrays.copyOf(activeSecret, activeSecret.length);
      final List<byte[]> list = new ArrayList<>(1 + historicalSecrets.size());
      list.add(Arrays.copyOf(activeSecret, activeSecret.length));
      for (byte[] hist : historicalSecrets) {
        list.add(Arrays.copyOf(hist, hist.length));
      }
      this.validationSecrets = Collections.unmodifiableList(list);
    }
  }

  private final Object lock = new Object();
  private final int maxHistoricalSecrets;
  private final Deque<byte[]> historicalSecrets = new ArrayDeque<>();
  private final AtomicReference<SecretSnapshot> snapshot;

  /**
   * Constructs a rotating secret provider with an initial secret and default history limit (3).
   *
   * @param initialSecret initial active secret
   */
  public RotatingTokenSecretProvider(byte @NonNull [] initialSecret) {
    this(initialSecret, 3);
  }

  /**
   * Constructs a rotating secret provider with initial secret and maximum retained history count.
   *
   * @param initialSecret initial active secret
   * @param maxHistoricalSecrets maximum count of previous secrets to retain for token validation
   */
  public RotatingTokenSecretProvider(byte @NonNull [] initialSecret, int maxHistoricalSecrets) {
    Objects.requireNonNull(initialSecret, "initialSecret must not be null");
    if (initialSecret.length < 16) {
      throw new IllegalArgumentException("Secret must be at least 16 bytes: " + initialSecret.length);
    }
    if (maxHistoricalSecrets < 0) {
      throw new IllegalArgumentException("maxHistoricalSecrets must be >= 0: " + maxHistoricalSecrets);
    }
    this.maxHistoricalSecrets = maxHistoricalSecrets;
    this.snapshot =
        new AtomicReference<>(new SecretSnapshot(initialSecret, historicalSecrets));
  }

  /**
   * Rotates to a new active secret, archiving the previous active secret to the historical list.
   *
   * @param newSecret newly issued active secret (at least 16 bytes)
   */
  public void rotateSecret(byte @NonNull [] newSecret) {
    Objects.requireNonNull(newSecret, "newSecret must not be null");
    if (newSecret.length < 16) {
      throw new IllegalArgumentException("Secret must be at least 16 bytes: " + newSecret.length);
    }
    EventLoopSafety.requireBlockingAllowed();
    synchronized (lock) {
      byte[] currentActive = snapshot.get().activeSecret;
      if (maxHistoricalSecrets > 0) {
        historicalSecrets.addFirst(currentActive);
        while (historicalSecrets.size() > maxHistoricalSecrets) {
          historicalSecrets.removeLast();
        }
      }
      this.snapshot.set(new SecretSnapshot(newSecret, historicalSecrets));
    }
  }

  @Override
  public byte @NonNull [] getActiveSecret() {
    byte[] active = snapshot.get().activeSecret;
    return Arrays.copyOf(active, active.length);
  }

  @Override
  public @NonNull List<byte[]> getValidationSecrets() {
    List<byte[]> source = snapshot.get().validationSecrets;
    List<byte[]> copy = new ArrayList<>(source.size());
    for (byte[] secret : source) {
      copy.add(Arrays.copyOf(secret, secret.length));
    }
    return Collections.unmodifiableList(copy);
  }
}

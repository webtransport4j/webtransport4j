package io.github.webtransport4j.api;

import org.jspecify.annotations.NonNull;

/**
 * A no-operation implementation of {@link WebTransportMetricsListener}. Used as the default
 * listener so that no null-checks are required on hot paths.
 *
 * @author https://github.com/sanjomo
 */
public final class NoOpWebTransportMetricsListener implements WebTransportMetricsListener {

  public static final NoOpWebTransportMetricsListener INSTANCE =
      new NoOpWebTransportMetricsListener();

  private NoOpWebTransportMetricsListener() {}

  @Override
  public void onSessionOpened(long sessionId, @NonNull String path) {}

  @Override
  public void onSessionClosed(long sessionId, int closeCode) {}

  @Override
  public void onStreamOpened(long sessionId, long streamId, boolean bidirectional) {}

  @Override
  public void onStreamClosed(long sessionId, long streamId) {}

  @Override
  public void onDatagramSent(long sessionId, int bytes) {}

  @Override
  public void onDatagramReceived(long sessionId, int bytes) {}

  @Override
  public void onDatagramDiscarded(long sessionId, @NonNull String reason) {}

  @Override
  public void onConnectionMigration(
      long sessionId, @NonNull String oldAddress, @NonNull String newAddress) {}
}

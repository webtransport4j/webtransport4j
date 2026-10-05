package io.github.webtransport4j.concurrency;

import io.github.webtransport4j.api.WebTransportMetricsListener;

/** No-op callback base for tests that exercise only selected metrics. */
public class MetricsAdapter implements WebTransportMetricsListener {
  public void onSessionOpened(long id, String path) {}

  public void onSessionClosed(long id, int code) {}

  public void onStreamOpened(long id, long stream, boolean bidi) {}

  public void onStreamClosed(long id, long stream) {}

  public void onDatagramSent(long id, int bytes) {}

  public void onDatagramReceived(long id, int bytes) {}

  public void onDatagramDiscarded(long id, String reason) {}

  public void onConnectionMigration(long id, String oldAddress, String newAddress) {}
}

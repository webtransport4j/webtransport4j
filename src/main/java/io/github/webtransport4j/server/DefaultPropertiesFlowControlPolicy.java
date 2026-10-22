package io.github.webtransport4j.server;

import io.github.webtransport4j.api.SessionFlowControlPolicy;
import io.github.webtransport4j.api.WebTransportSession;
import org.jspecify.annotations.NonNull;

/**
 * Default property-backed implementation of {@link SessionFlowControlPolicy}.
 * Extends session data and stream limits using static configuration values.
 */
public class DefaultPropertiesFlowControlPolicy implements SessionFlowControlPolicy {

  public static final DefaultPropertiesFlowControlPolicy INSTANCE =
      new DefaultPropertiesFlowControlPolicy();

  @Override
  public long onRequestDataCredit(
      @NonNull WebTransportSession session, long blockedAtLimit, long cumulativeReceivedBytes) {
    long extendAmount =
        WebTransportConfig.getLong(
            "webtransport4j.webtransport.flowcontrol.extend.data", 2097152L);
    long currentSettingsMaxData = session.getSettingsMaxData();
    long unconsumedCredit = currentSettingsMaxData - cumulativeReceivedBytes;
    if (extendAmount > 0
        && blockedAtLimit >= currentSettingsMaxData
        && unconsumedCredit < extendAmount) {
      return extendAmount;
    }
    return 0L;
  }

  @Override
  public long onRequestStreamCredit(
      @NonNull WebTransportSession session,
      boolean bidirectional,
      long blockedAtLimit,
      long currentActiveCount) {
    long initialLimit =
        bidirectional
            ? session.getInitialMaxStreamsBidi()
            : session.getInitialMaxStreamsUni();
    long remaining = initialLimit - currentActiveCount;
    long maxActiveStreams =
        WebTransportConfig.getLong(
            bidirectional
                ? "webtransport4j.webtransport.flowcontrol.max.active.streams.bidi"
                : "webtransport4j.webtransport.flowcontrol.max.active.streams.uni",
            100L);
    long cappedRemaining = Math.min(remaining, maxActiveStreams);
    if (cappedRemaining > 0) {
      long maxAbsolute =
          WebTransportConfig.getLong(
              bidirectional
                  ? "webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi"
                  : "webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni",
              5000L);
      return Math.min(blockedAtLimit + cappedRemaining, maxAbsolute);
    }
    return 0L;
  }

  @Override
  public long getMaxPendingOutboundBytes() {
    return WebTransportConfig.getLong(
        "webtransport4j.webtransport.flowcontrol.max.pending.outbound.bytes",
        64L * 1024 * 1024);
  }
}

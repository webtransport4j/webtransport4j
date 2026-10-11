package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportMetricsListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically reads Linux kernel UDP error counters from {@code /proc/net/snmp} and publishes
 * deltas to registered {@link WebTransportMetricsListener}s.
 *
 * <p>The counters are host-wide (not per-socket): {@code Udp: RcvbufErrors} counts datagrams the
 * kernel dropped because a socket receive buffer was full, and {@code Udp: InErrors} counts other
 * receive failures. Inbound datagrams are dispatched directly on the event loop with no transport
 * queue, so these kernel counters are the only place overload loss is observable before packets
 * reach the process.
 *
 * <p>The poller runs on a single shared daemon thread while at least one listener is registered,
 * and only on Linux when {@code /proc/net/snmp} is readable; everywhere else it is a no-op. The
 * cadence is configured with {@value #POLL_SECONDS_KEY} (default {@value #DEFAULT_POLL_SECONDS}
 * seconds).
 */
public final class UdpErrorMetricsPoller {

  /** Config key (seconds) controlling how often kernel UDP error counters are sampled. */
  public static final String POLL_SECONDS_KEY = "webtransport4j.metrics.udp.poll.seconds";

  /** Default poll cadence in seconds. */
  public static final int DEFAULT_POLL_SECONDS = 30;

  private static final Logger logger = LoggerFactory.getLogger(UdpErrorMetricsPoller.class);
  private static final Path SNMP_PATH = Paths.get("/proc/net/snmp");
  private static final UdpErrorMetricsPoller INSTANCE = new UdpErrorMetricsPoller();

  private final ConcurrentMap<Object, Supplier<WebTransportMetricsListener>> listeners =
      new ConcurrentHashMap<>();
  private final Object lifecycleLock = new Object();
  private Thread thread;
  private boolean running;
  private long lastRcvbufErrors = -1L;
  private long lastInErrors = -1L;

  private UdpErrorMetricsPoller() {}

  /**
   * Registers a listener supplier resolved on every poll. Re-registering the same {@code key}
   * replaces the previous supplier. Callers must {@link #unregister(Object)} the same key when
   * reporting should stop.
   */
  public static void register(
      @NonNull Object key, @NonNull Supplier<WebTransportMetricsListener> listenerSupplier) {
    INSTANCE.add(key, listenerSupplier);
  }

  /** Stops reporting to the listener supplier registered under {@code key}. */
  public static void unregister(@NonNull Object key) {
    INSTANCE.remove(key);
  }

  private void add(Object key, Supplier<WebTransportMetricsListener> listenerSupplier) {
    listeners.put(key, listenerSupplier);
    synchronized (lifecycleLock) {
      if (!isSupported()) {
        return;
      }
      running = true;
      if (thread == null) {
        Thread t = new Thread(this::runLoop, "wt-udp-error-metrics-poller");
        t.setDaemon(true);
        thread = t;
        t.start();
      }
    }
  }

  private void remove(Object key) {
    listeners.remove(key);
    synchronized (lifecycleLock) {
      if (listeners.isEmpty()) {
        running = false;
        LockSupport.unpark(thread);
      }
    }
  }

  private void runLoop() {
    for (; ; ) {
      synchronized (lifecycleLock) {
        if (!running || listeners.isEmpty()) {
          running = false;
          thread = null;
          return;
        }
      }
      try {
        pollOnce();
      } catch (Throwable failure) {
        logger.debug("Kernel UDP error counter poll failed", failure);
      }
      long seconds = WebTransportConfig.getLong(POLL_SECONDS_KEY, DEFAULT_POLL_SECONDS);
      if (seconds <= 0) {
        seconds = DEFAULT_POLL_SECONDS;
      }
      LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(seconds));
    }
  }

  private void pollOnce() {
    long[] counters = readUdpErrorCounters(SNMP_PATH);
    if (counters == null) {
      return;
    }
    long rcvbufErrors = counters[0];
    long inErrors = counters[1];
    long rcvbufDelta = delta(lastRcvbufErrors, rcvbufErrors);
    long inErrorsDelta = delta(lastInErrors, inErrors);
    lastRcvbufErrors = rcvbufErrors;
    lastInErrors = inErrors;
    if (rcvbufDelta <= 0 && inErrorsDelta <= 0) {
      return;
    }
    Set<WebTransportMetricsListener> targets = new HashSet<>();
    for (Supplier<WebTransportMetricsListener> supplier : listeners.values()) {
      try {
        WebTransportMetricsListener listener = supplier.get();
        if (listener != null) {
          targets.add(listener);
        }
      } catch (RuntimeException failure) {
        logger.debug("Metrics listener supplier failed", failure);
      }
    }
    for (WebTransportMetricsListener listener : targets) {
      if (rcvbufDelta > 0) {
        try {
          listener.onUdpReceiveBufferErrors(rcvbufDelta);
        } catch (Throwable failure) {
          logger.debug("Metrics listener failed in onUdpReceiveBufferErrors", failure);
        }
      }
      if (inErrorsDelta > 0) {
        try {
          listener.onUdpReceiveErrors(inErrorsDelta);
        } catch (Throwable failure) {
          logger.debug("Metrics listener failed in onUdpReceiveErrors", failure);
        }
      }
    }
  }

  private static long delta(long previous, long current) {
    if (current < 0 || previous < 0 || current < previous) {
      return 0;
    }
    return current - previous;
  }

  private static boolean isSupported() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    return os.contains("linux") && Files.isReadable(SNMP_PATH);
  }

  /**
   * Reads the {@code Udp:} RcvbufErrors and InErrors counters, or {@code null} when they are
   * unavailable. Package-private for tests.
   */
  static @Nullable long[] readUdpErrorCounters(@NonNull Path path) {
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII)) {
      return parseUdpErrorCounters(reader);
    } catch (IOException | RuntimeException e) {
      return null;
    }
  }

  /**
   * Parses {@code /proc/net/snmp} content for the {@code Udp:} section and returns {@code
   * [RcvbufErrors, InErrors]} (each {@code -1} when its column is absent), or {@code null} when
   * no Udp section is present. Package-private for tests.
   */
  static @Nullable long[] parseUdpErrorCounters(@NonNull BufferedReader reader)
      throws IOException {
    int rcvbufErrorsColumn = -1;
    int inErrorsColumn = -1;
    String line;
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("Udp:")) {
        continue;
      }
      String[] tokens = line.trim().split("\\s+");
      if (tokens.length > 1 && "InDatagrams".equals(tokens[1])) {
        for (int i = 1; i < tokens.length; i++) {
          if ("RcvbufErrors".equals(tokens[i])) {
            rcvbufErrorsColumn = i;
          } else if ("InErrors".equals(tokens[i])) {
            inErrorsColumn = i;
          }
        }
      } else if (rcvbufErrorsColumn > 0 || inErrorsColumn > 0) {
        return new long[] {
          parseCounter(tokens, rcvbufErrorsColumn), parseCounter(tokens, inErrorsColumn)
        };
      }
    }
    return null;
  }

  private static long parseCounter(String[] tokens, int column) {
    if (column <= 0 || column >= tokens.length) {
      return -1L;
    }
    try {
      return Long.parseLong(tokens[column]);
    } catch (NumberFormatException e) {
      return -1L;
    }
  }
}

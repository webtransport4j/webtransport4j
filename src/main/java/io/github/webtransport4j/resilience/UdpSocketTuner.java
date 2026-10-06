package io.github.webtransport4j.resilience;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Locale;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operating system UDP socket auto-tuner for high-throughput WebTransport packet delivery.
 *
 * <p>Under high-throughput datagram loads (e.g. 100k+ datagrams/sec), default OS socket buffers
 * (often 208 KB on Linux or 64 KB on Windows) overflow, causing kernel-level UDP packet drops.
 * This utility recommends {@code SO_RCVBUF} and {@code SO_SNDBUF} requests up to 4 MiB, capped
 * by detected Linux limits. The kernel determines the actual allocated socket buffer sizes.
 */
public final class UdpSocketTuner {

  private static final Logger logger = LoggerFactory.getLogger(UdpSocketTuner.class);

  /** Default recommended buffer size: 4 MB. */
  public static final int RECOMMENDED_BUFFER_SIZE = 4 * 1024 * 1024;

  /** Desired minimum buffer size: 1 MiB; lower detected OS limits take precedence. */
  public static final int MIN_BUFFER_SIZE = 1024 * 1024;

  private static final int DETECTED_RCVBUF;
  private static final int DETECTED_SNDBUF;

  static {
    int maxRcv = detectLinuxProcLimit("/proc/sys/net/core/rmem_max");
    int maxSnd = detectLinuxProcLimit("/proc/sys/net/core/wmem_max");

    final String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("linux")) {
      DETECTED_RCVBUF = recommendLinuxBufferSize(maxRcv, "net.core.rmem_max");
      DETECTED_SNDBUF = recommendLinuxBufferSize(maxSnd, "net.core.wmem_max");
    } else if (os.contains("mac") || os.contains("darwin")) {
      // macOS default kern.ipc.maxsockbuf is typically 4MB to 8MB
      DETECTED_RCVBUF = Math.min(RECOMMENDED_BUFFER_SIZE, 4 * 1024 * 1024);
      DETECTED_SNDBUF = Math.min(RECOMMENDED_BUFFER_SIZE, 4 * 1024 * 1024);
    } else {
      // Windows or other OS
      DETECTED_RCVBUF = 2 * 1024 * 1024;
      DETECTED_SNDBUF = 2 * 1024 * 1024;
    }
  }

  private UdpSocketTuner() {}

  /**
   * Applies recommended socket buffer requests to unset options on the server {@link Bootstrap}.
   * Explicit send and receive options are preserved independently.
   *
   * @param bootstrap the server bootstrap to tune
   */
  public static void tune(@NonNull Bootstrap bootstrap) {
    final int rcv = getRecommendedReceiveBufferSize();
    final int snd = getRecommendedSendBufferSize();

    if (!bootstrap.config().options().containsKey(ChannelOption.SO_RCVBUF)) {
      bootstrap.option(ChannelOption.SO_RCVBUF, rcv);
    }
    if (!bootstrap.config().options().containsKey(ChannelOption.SO_SNDBUF)) {
      bootstrap.option(ChannelOption.SO_SNDBUF, snd);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Requested UDP socket options: SO_RCVBUF={} bytes, SO_SNDBUF={} bytes",
          bootstrap.config().options().get(ChannelOption.SO_RCVBUF),
          bootstrap.config().options().get(ChannelOption.SO_SNDBUF));
    }
  }

  /**
   * Returns the recommended SO_RCVBUF size in bytes.
   *
   * @return recommended receive buffer size
   */
  public static int getRecommendedReceiveBufferSize() {
    return DETECTED_RCVBUF;
  }

  /**
   * Returns the recommended SO_SNDBUF size in bytes.
   *
   * @return recommended send buffer size
   */
  public static int getRecommendedSendBufferSize() {
    return DETECTED_SNDBUF;
  }

  /** Caps the target at a known Linux limit and reports limits below the desired minimum. */
  static int recommendLinuxBufferSize(int limit, String setting) {
    if (limit <= 0) {
      return RECOMMENDED_BUFFER_SIZE;
    }
    if (limit < MIN_BUFFER_SIZE) {
      logger.warn("UDP socket buffer limit {}={} bytes is below the {} byte target; "
          + "raise the OS limit to meet the target", setting, limit, MIN_BUFFER_SIZE);
    }
    return Math.min(RECOMMENDED_BUFFER_SIZE, limit);
  }

  private static int detectLinuxProcLimit(String path) {
    try {
      final File file = new File(path);
      if (file.exists() && file.canRead()) {
        try (BufferedReader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
          final String line = reader.readLine();
          if (line != null && !line.trim().isEmpty()) {
            return Integer.parseInt(line.trim());
          }
        }
      }
    } catch (Exception ignored) {
      // Non-Linux or restricted container
    }
    return -1;
  }
}

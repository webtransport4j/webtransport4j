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
 * This utility inspects OS limits and scales {@code SO_RCVBUF} and {@code SO_SNDBUF} up to 4 MB - 16 MB.
 */
public final class UdpSocketTuner {

  private static final Logger logger = LoggerFactory.getLogger(UdpSocketTuner.class);

  /** Default recommended buffer size: 4 MB. */
  public static final int RECOMMENDED_BUFFER_SIZE = 4 * 1024 * 1024;

  /** Minimum acceptable buffer size: 1 MB. */
  public static final int MIN_BUFFER_SIZE = 1024 * 1024;

  private static final int DETECTED_RCVBUF;
  private static final int DETECTED_SNDBUF;

  static {
    int maxRcv = detectLinuxProcLimit("/proc/sys/net/core/rmem_max");
    int maxSnd = detectLinuxProcLimit("/proc/sys/net/core/wmem_max");

    final String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("linux")) {
      DETECTED_RCVBUF = maxRcv > 0 ? Math.min(RECOMMENDED_BUFFER_SIZE, maxRcv) : RECOMMENDED_BUFFER_SIZE;
      DETECTED_SNDBUF = maxSnd > 0 ? Math.min(RECOMMENDED_BUFFER_SIZE, maxSnd) : RECOMMENDED_BUFFER_SIZE;
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
   * Applies optimal socket buffer sizes to the Netty server {@link Bootstrap}.
   *
   * @param bootstrap the server bootstrap to tune
   */
  public static void tune(@NonNull Bootstrap bootstrap) {
    final int rcv = getRecommendedReceiveBufferSize();
    final int snd = getRecommendedSendBufferSize();

    bootstrap.option(ChannelOption.SO_RCVBUF, rcv);
    bootstrap.option(ChannelOption.SO_SNDBUF, snd);

    if (logger.isDebugEnabled()) {
      logger.debug("⚡ Auto-tuned UDP socket options: SO_RCVBUF={} bytes, SO_SNDBUF={} bytes", rcv, snd);
    }
  }

  /**
   * Returns the recommended SO_RCVBUF size in bytes.
   *
   * @return recommended receive buffer size
   */
  public static int getRecommendedReceiveBufferSize() {
    return Math.max(MIN_BUFFER_SIZE, DETECTED_RCVBUF);
  }

  /**
   * Returns the recommended SO_SNDBUF size in bytes.
   *
   * @return recommended send buffer size
   */
  public static int getRecommendedSendBufferSize() {
    return Math.max(MIN_BUFFER_SIZE, DETECTED_SNDBUF);
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

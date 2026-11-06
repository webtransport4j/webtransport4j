package io.github.webtransport4j.server;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicStreamChannel;
import org.jspecify.annotations.NonNull;

/**
 * Strategy SPI abstracting how outbound WebTransport stream resets ({@code RESET_STREAM_AT} /
 * {@code RESET_STREAM}) are dispatched to the underlying QUIC transport.
 *
 * <p>Per Draft-17 Section 4.4, resetting the send side of a WebTransport stream maps the 32-bit
 * application error code into the {@code WT_APPLICATION_ERROR} range and computes a Reliable Size
 * equal to the local WebTransport stream header length plus any application offset. Isolating the
 * transport invocation behind this interface keeps session/stream code decoupled from Netty QUIC's
 * native method signature and allows deterministic unit testing of both error mapping and reliable
 * offset calculation.
 */
@FunctionalInterface
public interface StreamResetTransport {

  /**
   * Default Netty QUIC transport adapter.
   *
   * <p>Netty {@code 4.2.x} {@link QuicStreamChannel} exposes {@code shutdownOutput(int,
   * ChannelPromise)} without a {@code RESET_STREAM_AT} {@code reliableSize} parameter and
   * sign-extends negative 32-bit {@code int} values in JNI. Until Netty QUIC exposes unsigned
   * 62-bit {@code RESET_STREAM_AT}, negative 32-bit casts fall back to the 31-bit non-negative
   * {@code appErrorCode} (or {@code 0}) to prevent native Quiche panics. Calling {@code
   * shutdownOutput} resets the send side while preserving the receive side per Draft-17 Section
   * 4.4.
   */
  StreamResetTransport NETTY_DEFAULT =
      (streamChannel, appErrorCode, mappedHttpErrorCode, wireReliableSize) -> {
        int code = (int) mappedHttpErrorCode;
        if (code < 0) {
          code =
              (appErrorCode >= 0L && appErrorCode <= Integer.MAX_VALUE) ? (int) appErrorCode : 0;
        }
        ChannelPromise promise = streamChannel.newPromise();
        streamChannel.shutdownOutput(code, promise);
        return promise;
      };

  /**
   * Resets the send side of the given QUIC stream channel.
   *
   * @param streamChannel underlying QUIC stream channel
   * @param appErrorCode 32-bit unsigned WebTransport application error code ({@code
   *     0x00000000..0xffffffff})
   * @param mappedHttpErrorCode mapped 62-bit {@code WT_APPLICATION_ERROR} HTTP/3 wire code
   * @param wireReliableSize exact {@code RESET_STREAM_AT} Reliable Size in bytes (header + offset)
   * @return future completing when the transport reset operation is scheduled/completed
   */
  @NonNull ChannelFuture resetSendSide(
      @NonNull QuicStreamChannel streamChannel,
      long appErrorCode,
      long mappedHttpErrorCode,
      long wireReliableSize);
}


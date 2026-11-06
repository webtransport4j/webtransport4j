package io.github.webtransport4j.api;

import java.io.IOException;
import java.util.OptionalLong;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Signals that a WebTransport data stream was reset by the peer via {@code RESET_STREAM}, {@code
 * RESET_STREAM_AT}, or {@code STOP_SENDING} per Draft-17 Section 4.4.
 *
 * <p>If the peer's wire error code falls within the {@code WT_APPLICATION_ERROR} range, {@link
 * #applicationErrorCode()} contains the decoded 32-bit unsigned WebTransport application error code
 * ({@code 0x00000000..0xffffffff}). If the wire error code is outside {@code WT_APPLICATION_ERROR}
 * (for example {@code H3_REQUEST_CANCELLED} or {@code WT_BUFFERED_STREAM_REJECTED}), the stream is
 * still reset, and {@link #applicationErrorCode()} is empty.
 */
public final class WebTransportStreamResetException extends IOException {

  private static final long serialVersionUID = 1L;

  private final long rawHttp3ErrorCode;
  private final boolean hasAppErrorCode;
  private final long appErrorCode;

  private WebTransportStreamResetException(
      @NonNull String message,
      long rawHttp3ErrorCode,
      boolean hasAppErrorCode,
      long appErrorCode,
      @Nullable Throwable cause) {
    super(message, cause);
    this.rawHttp3ErrorCode = rawHttp3ErrorCode;
    this.hasAppErrorCode = hasAppErrorCode;
    this.appErrorCode = appErrorCode;
  }

  /**
   * Creates a reset exception carrying a mapped 32-bit WebTransport application error code.
   *
   * @param rawHttp3ErrorCode raw HTTP/3 wire error code received from the transport
   * @param applicationErrorCode decoded 32-bit WebTransport application error code
   * @param cause optional underlying transport exception
   * @return constructed exception instance
   */
  public static @NonNull WebTransportStreamResetException withApplicationError(
      long rawHttp3ErrorCode, long applicationErrorCode, @Nullable Throwable cause) {
    return new WebTransportStreamResetException(
        "WebTransport stream reset with application error code: " + applicationErrorCode,
        rawHttp3ErrorCode,
        true,
        applicationErrorCode,
        cause);
  }

  /**
   * Creates a reset exception with no mapped application error code (wire code outside {@code
   * WT_APPLICATION_ERROR}).
   *
   * @param rawHttp3ErrorCode raw HTTP/3 wire error code received from the transport
   * @param cause optional underlying transport exception
   * @return constructed exception instance
   */
  public static @NonNull WebTransportStreamResetException withoutApplicationError(
      long rawHttp3ErrorCode, @Nullable Throwable cause) {
    return new WebTransportStreamResetException(
        "WebTransport stream reset without application error code (HTTP/3 code 0x"
            + Long.toHexString(rawHttp3ErrorCode)
            + ")",
        rawHttp3ErrorCode,
        false,
        0L,
        cause);
  }

  /**
   * Returns the decoded 32-bit WebTransport application error code if present, or {@link
   * OptionalLong#empty()} when the peer reset the stream with a non-application HTTP/3 error code.
   *
   * @return optional 32-bit unsigned application error code
   */
  public @NonNull OptionalLong applicationErrorCode() {
    return hasAppErrorCode ? OptionalLong.of(appErrorCode) : OptionalLong.empty();
  }

  /**
   * Returns whether this reset carries a mapped {@code WT_APPLICATION_ERROR} code.
   *
   * @return {@code true} if an application error code is present
   */
  public boolean hasApplicationErrorCode() {
    return hasAppErrorCode;
  }

  /**
   * Returns the raw HTTP/3 wire error code reported by the underlying QUIC stream reset frame.
   *
   * @return raw 62-bit HTTP/3 wire error code
   */
  public long rawHttp3ErrorCode() {
    return rawHttp3ErrorCode;
  }
}


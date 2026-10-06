package io.github.webtransport4j.api;

/**
 * Exception thrown when an outbound write operation is rejected because the stream's write buffer
 * capacity has been exceeded and backpressure is exerted.
 */
public class StreamBackpressureException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new {@link StreamBackpressureException} with the specified detail message.
   *
   * @param message detail message explaining the backpressure condition
   */
  public StreamBackpressureException(String message) {
    super(message);
  }

  /**
   * Constructs a new {@link StreamBackpressureException} with the specified detail message and
   * cause.
   *
   * @param message detail message explaining the backpressure condition
   * @param cause underlying cause
   */
  public StreamBackpressureException(String message, Throwable cause) {
    super(message, cause);
  }
}

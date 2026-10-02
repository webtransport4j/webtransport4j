package io.github.webtransport4j.api;

/**
 * Summary of an active WebTransport stream within a session.
 *
 * <p>Provides a read-only snapshot of stream characteristics and state
 * without exposing transport-specific channel primitives.
 *
 * @author https://github.com/sanjomo
 */
public interface WebTransportStreamSummary {

  /**
   * Returns the unique stream identifier.
   *
   * @return the stream ID
   */
  long streamId();

  /**
   * Returns true if the stream is bidirectional, false if unidirectional.
   *
   * @return true for bidirectional stream
   */
  boolean isBidirectional();

  /**
   * Returns true if the stream was initiated locally by this node, false if peer-initiated.
   *
   * @return true if locally created
   */
  boolean isLocalCreated();

  /**
   * Returns true if the stream channel is currently active.
   *
   * @return true if active
   */
  boolean isActive();

  /**
   * Returns true if the stream channel is open.
   *
   * @return true if open
   */
  boolean isOpen();

  /**
   * Returns true if the stream channel is writable.
   *
   * @return true if writable
   */
  boolean isWritable();
}

package io.github.webtransport4j.afxdp;

import java.io.IOException;

/**
 * Exception thrown when an AF_XDP transport or kernel operation fails.
 */
public class AfXdpException extends IOException {

  private static final long serialVersionUID = 1L;

  private final int errno;

  public AfXdpException(String message) {
    super(message);
    this.errno = 0;
  }

  public AfXdpException(String message, int errno) {
    super(message);
    this.errno = errno;
  }

  public AfXdpException(String message, Throwable cause) {
    super(message, cause);
    this.errno = 0;
  }

  /**
   * Returns the Linux errno associated with the failure, or 0 if unknown.
   */
  public int getErrno() {
    return errno;
  }
}

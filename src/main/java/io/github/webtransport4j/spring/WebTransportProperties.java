package io.github.webtransport4j.spring;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for WebTransport4J in Spring Boot.
 * Binds to properties prefixed with {@code webtransport4j.*}.
 */
public class WebTransportProperties {

  private int port = 4433;
  private String sslKeyPath;
  private String sslCertPath;
  private List<String> allowedOrigins = new ArrayList<>();
  private String transport = "auto";
  private long idleTimeoutSeconds = 60;
  private long maxStreamsBidi = 100;
  private long maxStreamsUni = 100;
  private long maxData = 10485760; // 10MB default

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getSslKeyPath() {
    return sslKeyPath;
  }

  public void setSslKeyPath(String sslKeyPath) {
    this.sslKeyPath = sslKeyPath;
  }

  public String getSslCertPath() {
    return sslCertPath;
  }

  public void setSslCertPath(String sslCertPath) {
    this.sslCertPath = sslCertPath;
  }

  public List<String> getAllowedOrigins() {
    return allowedOrigins;
  }

  public void setAllowedOrigins(List<String> allowedOrigins) {
    this.allowedOrigins = allowedOrigins;
  }

  public String getTransport() {
    return transport;
  }

  public void setTransport(String transport) {
    this.transport = transport;
  }

  public long getIdleTimeoutSeconds() {
    return idleTimeoutSeconds;
  }

  public void setIdleTimeoutSeconds(long idleTimeoutSeconds) {
    this.idleTimeoutSeconds = idleTimeoutSeconds;
  }

  public long getMaxStreamsBidi() {
    return maxStreamsBidi;
  }

  public void setMaxStreamsBidi(long maxStreamsBidi) {
    this.maxStreamsBidi = maxStreamsBidi;
  }

  public long getMaxStreamsUni() {
    return maxStreamsUni;
  }

  public void setMaxStreamsUni(long maxStreamsUni) {
    this.maxStreamsUni = maxStreamsUni;
  }

  public long getMaxData() {
    return maxData;
  }

  public void setMaxData(long maxData) {
    this.maxData = maxData;
  }
}

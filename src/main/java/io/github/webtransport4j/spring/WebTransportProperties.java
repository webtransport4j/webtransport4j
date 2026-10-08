package io.github.webtransport4j.spring;

import io.github.webtransport4j.server.WebTransportConfig;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;

/**
 * Configuration properties for WebTransport4J in Spring Boot. Binds to properties prefixed with
 * {@code webtransport4j.*}.
 */
public class WebTransportProperties {

  private int port =
      Integer.parseInt(
          System.getProperty(
              "webtransport4j.port",
              WebTransportConfig.get("webtransport4j.server.port", "4433")));
  private String sslKeyPath =
      System.getProperty(
          "webtransport4j.ssl_key_path",
          WebTransportConfig.get("webtransport4j.server.ssl_key_path", null));
  private String sslCertPath =
      System.getProperty(
          "webtransport4j.ssl_cert_path",
          WebTransportConfig.get("webtransport4j.server.ssl_cert_path", null));
  private List<String> allowedOrigins = new ObjectArrayList<>();
  private String transport =
      System.getProperty(
          "webtransport4j.transport",
          WebTransportConfig.get("webtransport4j.server.transport", "auto"));
  private long idleTimeoutSeconds =
      Long.parseLong(
          System.getProperty(
              "webtransport4j.idle_timeout_seconds",
              WebTransportConfig.get("webtransport4j.quic.max_idle_timeout_seconds", "60")));
  private long maxStreamsBidi =
      Long.parseLong(
          System.getProperty(
              "webtransport4j.max_streams_bidi",
              WebTransportConfig.get("webtransport4j.quic.initial_max_streams_bidi", "100")));
  private long maxStreamsUni =
      Long.parseLong(
          System.getProperty(
              "webtransport4j.max_streams_uni",
              WebTransportConfig.get("webtransport4j.quic.initial_max_streams_uni", "100")));
  private long maxData =
      Long.parseLong(
          System.getProperty(
              "webtransport4j.max_data",
              WebTransportConfig.get("webtransport4j.quic.initial_max_data", "10485760"))); // 10MB default

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

  private int capsuleMaxLength =
      Integer.parseInt(
          System.getProperty(
              "webtransport4j.capsule.max_length",
              WebTransportConfig.get("webtransport4j.capsule.max_length", "65536")));

  public long getMaxData() {
    return maxData;
  }

  public void setMaxData(long maxData) {
    this.maxData = maxData;
  }

  public int getCapsuleMaxLength() {
    return capsuleMaxLength;
  }

  public void setCapsuleMaxLength(int capsuleMaxLength) {
    this.capsuleMaxLength = capsuleMaxLength;
  }
}

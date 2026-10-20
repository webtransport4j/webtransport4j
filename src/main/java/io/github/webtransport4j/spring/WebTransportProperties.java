package io.github.webtransport4j.spring;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Configuration properties for WebTransport4J in Spring Boot. Binds to properties prefixed with
 * {@code webtransport4j.*}.
 */
public class WebTransportProperties {

  private @Nullable Integer port;
  private @Nullable String sslKeyPath;
  private @Nullable String sslCertPath;
  private List<String> allowedOrigins = new ObjectArrayList<>();
  private @Nullable String transport;
  private @Nullable Long idleTimeoutSeconds;
  private @Nullable Long maxStreamsBidi;
  private @Nullable Long maxStreamsUni;
  private @Nullable Long maxData;
  private @Nullable Integer capsuleMaxLength;

  public @Nullable Integer getPort() {
    return port;
  }

  public void setPort(@Nullable Integer port) {
    this.port = port;
  }

  public @Nullable String getSslKeyPath() {
    return sslKeyPath;
  }

  public void setSslKeyPath(@Nullable String sslKeyPath) {
    this.sslKeyPath = sslKeyPath;
  }

  public @Nullable String getSslCertPath() {
    return sslCertPath;
  }

  public void setSslCertPath(@Nullable String sslCertPath) {
    this.sslCertPath = sslCertPath;
  }

  public List<String> getAllowedOrigins() {
    return allowedOrigins;
  }

  public void setAllowedOrigins(List<String> allowedOrigins) {
    this.allowedOrigins = allowedOrigins;
  }

  public @Nullable String getTransport() {
    return transport;
  }

  public void setTransport(@Nullable String transport) {
    this.transport = transport;
  }

  public @Nullable Long getIdleTimeoutSeconds() {
    return idleTimeoutSeconds;
  }

  public void setIdleTimeoutSeconds(@Nullable Long idleTimeoutSeconds) {
    this.idleTimeoutSeconds = idleTimeoutSeconds;
  }

  public @Nullable Long getMaxStreamsBidi() {
    return maxStreamsBidi;
  }

  public void setMaxStreamsBidi(@Nullable Long maxStreamsBidi) {
    this.maxStreamsBidi = maxStreamsBidi;
  }

  public @Nullable Long getMaxStreamsUni() {
    return maxStreamsUni;
  }

  public void setMaxStreamsUni(@Nullable Long maxStreamsUni) {
    this.maxStreamsUni = maxStreamsUni;
  }

  public @Nullable Long getMaxData() {
    return maxData;
  }

  public void setMaxData(@Nullable Long maxData) {
    this.maxData = maxData;
  }

  public @Nullable Integer getCapsuleMaxLength() {
    return capsuleMaxLength;
  }

  public void setCapsuleMaxLength(@Nullable Integer capsuleMaxLength) {
    this.capsuleMaxLength = capsuleMaxLength;
  }
}

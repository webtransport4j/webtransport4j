package io.github.webtransport4j.spring;

import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.netty.handler.codec.quic.QuicTokenHandler;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;

/**
 * Spring Auto-Configuration for WebTransport4J. Discovers handlers, endpoints annotated with {@link
 * WebTransportEndpoint}, custom metric listeners, executors, and bootstraps {@link
 * WebTransportServer}.
 */
@Configuration
public class WebTransportAutoConfiguration implements ApplicationContextAware {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportAutoConfiguration.class);

  private ApplicationContext applicationContext;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  /**
   * Creates and configures the {@link WebTransportProperties} bean from the environment.
   *
   * @return initialized {@link WebTransportProperties} instance
   */
  @Bean
  public WebTransportProperties webTransportProperties() {
    WebTransportProperties properties = new WebTransportProperties();
    if (applicationContext != null && applicationContext.getEnvironment() != null) {
      bindEnvironment(properties, applicationContext.getEnvironment());
    }
    return properties;
  }

  private static void bindEnvironment(WebTransportProperties props, Environment env) {
    String portVal = getFirstProperty(env, "webtransport4j.port", "webtransport4j.server.port");
    if (portVal != null) {
      try {
        props.setPort(Integer.parseInt(portVal.trim()));
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }

    String sslKey =
        getFirstProperty(
            env,
            "webtransport4j.ssl-key-path",
            "webtransport4j.ssl_key_path",
            "webtransport4j.sslKeyPath",
            "webtransport4j.server.ssl_key_path");
    if (sslKey != null) {
      props.setSslKeyPath(sslKey.trim());
    }

    String sslCert =
        getFirstProperty(
            env,
            "webtransport4j.ssl-cert-path",
            "webtransport4j.ssl_cert_path",
            "webtransport4j.sslCertPath",
            "webtransport4j.server.ssl_cert_path");
    if (sslCert != null) {
      props.setSslCertPath(sslCert.trim());
    }

    String transport =
        getFirstProperty(
            env,
            "webtransport4j.transport",
            "webtransport4j.transport-type",
            "webtransport4j.server.transport");
    if (transport != null) {
      props.setTransport(transport.trim());
    }

    String idleTimeout =
        getFirstProperty(
            env,
            "webtransport4j.idle-timeout-seconds",
            "webtransport4j.idle_timeout_seconds",
            "webtransport4j.idleTimeoutSeconds");
    if (idleTimeout != null) {
      try {
        props.setIdleTimeoutSeconds(Long.parseLong(idleTimeout.trim()));
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }

    String bidi =
        getFirstProperty(
            env,
            "webtransport4j.max-streams-bidi",
            "webtransport4j.max_streams_bidi",
            "webtransport4j.maxStreamsBidi");
    if (bidi != null) {
      try {
        props.setMaxStreamsBidi(Long.parseLong(bidi.trim()));
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }

    String uni =
        getFirstProperty(
            env,
            "webtransport4j.max-streams-uni",
            "webtransport4j.max_streams_uni",
            "webtransport4j.maxStreamsUni");
    if (uni != null) {
      try {
        props.setMaxStreamsUni(Long.parseLong(uni.trim()));
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }

    String maxData =
        getFirstProperty(
            env,
            "webtransport4j.max-data",
            "webtransport4j.max_data",
            "webtransport4j.maxData");
    if (maxData != null) {
      try {
        props.setMaxData(Long.parseLong(maxData.trim()));
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }

    String capsuleMax =
        getFirstProperty(
            env,
            "webtransport4j.capsule-max-length",
            "webtransport4j.capsule.max_length",
            "webtransport4j.capsule.max-length",
            "webtransport4j.capsuleMaxLength");
    if (capsuleMax != null) {
      try {
        props.setCapsuleMaxLength(Integer.parseInt(capsuleMax.trim()));
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }

    String origins =
        getFirstProperty(
            env,
            "webtransport4j.allowed-origins",
            "webtransport4j.allowed_origins",
            "webtransport4j.allowedOrigins");
    if (origins != null && !origins.trim().isEmpty()) {
      String[] parts = origins.split(",");
      it.unimi.dsi.fastutil.objects.ObjectArrayList<String> list =
          new it.unimi.dsi.fastutil.objects.ObjectArrayList<>(parts.length);
      for (String p : parts) {
        String trimmed = p.trim();
        if (!trimmed.isEmpty()) {
          list.add(trimmed);
        }
      }
      props.setAllowedOrigins(list);
    }
  }

  private static String getFirstProperty(Environment env, String... keys) {
    for (String key : keys) {
      String val = env.getProperty(key);
      if (val != null && !val.trim().isEmpty()) {
        return val;
      }
    }
    return null;
  }

  /**
   * Configures and creates the {@link WebTransportServer} bean.
   *
   * @param properties WebTransport configuration properties
   * @param metricsListenerProvider optional metrics listener provider
   * @param tokenHandlerProvider optional token handler provider
   * @param executorProvider optional business executor provider
   * @return initialized WebTransportServer instance
   */
  @Bean
  public WebTransportServer webTransportServer(
      WebTransportProperties properties,
      ObjectProvider<WebTransportMetricsListener> metricsListenerProvider,
      ObjectProvider<QuicTokenHandler> tokenHandlerProvider,
      ObjectProvider<ExecutorService> executorProvider) {

    WebTransportServerBuilder builder = WebTransportServer.builder();

    builder
        .port(properties.getPort())
        .sslKeyPath(properties.getSslKeyPath())
        .sslCertPath(properties.getSslCertPath())
        .transportType(properties.getTransport())
        .idleTimeout(properties.getIdleTimeoutSeconds(), TimeUnit.SECONDS)
        .maxStreams(properties.getMaxStreamsBidi(), properties.getMaxStreamsUni())
        .maxData(properties.getMaxData());

    if (!properties.getAllowedOrigins().isEmpty()) {
      builder.allowedOrigins(properties.getAllowedOrigins());
    }

    metricsListenerProvider.ifAvailable(builder::metricsListener);
    tokenHandlerProvider.ifAvailable(builder::quicTokenHandler);
    executorProvider.ifAvailable(builder::businessExecutor);

    // Auto-discover @WebTransportEndpoint annotated beans or WebTransportHandler beans
    Map<String, Object> endpointBeans =
        applicationContext.getBeansWithAnnotation(WebTransportEndpoint.class);
    for (Map.Entry<String, Object> entry : endpointBeans.entrySet()) {
      Object bean = entry.getValue();
      WebTransportEndpoint ann = bean.getClass().getAnnotation(WebTransportEndpoint.class);
      if (ann == null) {
        // Class level annotation might be on target class if CGLIB proxy
        ann = AnnotationUtils.findAnnotation(bean.getClass(), WebTransportEndpoint.class);
      }
      if (ann != null) {
        String path = ann.path();
        boolean isDefault = ann.isDefault();
        logger.info(
            "📡 Discovered Spring WebTransport Endpoint: path='{}', default={}, bean='{}'",
            path,
            isDefault,
            entry.getKey());

        if (bean instanceof WebTransportHandler) {
          WebTransportHandler handler = (WebTransportHandler) bean;
          if (isDefault) {
            builder.defaultHandler(handler);
          } else {
            builder.handler(path, handler);
          }
        } else if (bean instanceof ReactiveWebTransportHandler) {
          ReactiveWebTransportHandler reactiveHandler = (ReactiveWebTransportHandler) bean;
          if (isDefault) {
            builder.defaultReactiveHandler(reactiveHandler);
          } else {
            builder.reactiveHandler(path, reactiveHandler);
          }
        } else {
          logger.warn(
              "⚠️ Bean '{}' is annotated with @WebTransportEndpoint but does not implement "
                  + "WebTransportHandler or ReactiveWebTransportHandler",
              entry.getKey());
        }
      }
    }

    // Also discover plain WebTransportHandler beans that are not annotated if no endpoint
    // annotation present
    Map<String, WebTransportHandler> handlers =
        applicationContext.getBeansOfType(WebTransportHandler.class);
    for (Map.Entry<String, WebTransportHandler> entry : handlers.entrySet()) {
      WebTransportHandler handler = entry.getValue();
      if (!endpointBeans.containsKey(entry.getKey())) {
        logger.info(
            "📡 Discovered unannotated WebTransportHandler bean '{}', registering at default path"
                + " '/'",
            entry.getKey());
        builder.defaultHandler(handler);
      }
    }

    return builder.build();
  }

  /**
   * Creates the lifecycle bean to start and stop the WebTransport server.
   *
   * @param server the WebTransportServer instance
   * @return lifecycle manager
   */
  @Bean
  public SpringWebTransportServerLifecycle springWebTransportServerLifecycle(
      WebTransportServer server) {
    return new SpringWebTransportServerLifecycle(server);
  }
}

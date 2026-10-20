package io.github.webtransport4j.spring;

import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.netty.handler.codec.quic.QuicTokenHandler;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;

/**
 * Spring Auto-Configuration for WebTransport4J. Discovers handlers, endpoints annotated with {@link
 * WebTransportEndpoint}, custom metric listeners, and bootstraps {@link WebTransportServer}.
 */
@AutoConfiguration
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
  @ConditionalOnMissingBean
  public WebTransportProperties webTransportProperties() {
    WebTransportProperties properties = new WebTransportProperties();
    if (applicationContext != null && applicationContext.getEnvironment() != null) {
      bindEnvironment(properties, applicationContext.getEnvironment());
    }
    return properties;
  }

  private static void bindEnvironment(WebTransportProperties props, Environment env) {
    String portVal =
        getFirstProperty(env, "webtransport4j.server.port", "webtransport4j.port");
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
            "webtransport4j.ssl.key.path",
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
            "webtransport4j.ssl.cert.path",
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
            "webtransport4j.server.transport",
            "webtransport4j.transport",
            "webtransport4j.transport-type");
    if (transport != null) {
      props.setTransport(transport.trim());
    }

    String idleTimeout =
        getFirstProperty(
            env,
            "webtransport4j.quic.idle.timeout.seconds",
            "webtransport4j.idle-timeout-seconds",
            "webtransport4j.idle_timeout_seconds",
            "webtransport4j.idleTimeoutSeconds",
            "webtransport4j.quic.max_idle_timeout_seconds");
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
            "webtransport4j.quic.max.streams.bidi",
            "webtransport4j.max-streams-bidi",
            "webtransport4j.max_streams_bidi",
            "webtransport4j.maxStreamsBidi",
            "webtransport4j.quic.initial_max_streams_bidi");
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
            "webtransport4j.quic.max.streams.uni",
            "webtransport4j.max-streams-uni",
            "webtransport4j.max_streams_uni",
            "webtransport4j.maxStreamsUni",
            "webtransport4j.quic.initial_max_streams_uni");
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
            "webtransport4j.quic.initial.max.data",
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
            "webtransport4j.capsule.max_length",
            "webtransport4j.capsule-max-length",
            "webtransport4j.capsule.max.length",
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
            "webtransport4j.allowed.origins",
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
   * @return initialized WebTransportServer instance
   */
  @Bean
  @ConditionalOnMissingBean
  public WebTransportServer webTransportServer(
      WebTransportProperties properties,
      ObjectProvider<WebTransportMetricsListener> metricsListenerProvider,
      ObjectProvider<QuicTokenHandler> tokenHandlerProvider) {

    WebTransportServerBuilder builder = WebTransportServer.builder();

    if (properties.getPort() != null) {
      builder.port(properties.getPort());
    }
    if (properties.getSslKeyPath() != null) {
      builder.sslKeyPath(properties.getSslKeyPath());
    }
    if (properties.getSslCertPath() != null) {
      builder.sslCertPath(properties.getSslCertPath());
    }
    if (properties.getTransport() != null) {
      builder.transportType(properties.getTransport());
    }
    if (properties.getIdleTimeoutSeconds() != null) {
      builder.idleTimeout(properties.getIdleTimeoutSeconds(), TimeUnit.SECONDS);
    }
    if (properties.getMaxStreamsBidi() != null) {
      builder.maxStreamsBidi(properties.getMaxStreamsBidi());
    }
    if (properties.getMaxStreamsUni() != null) {
      builder.maxStreamsUni(properties.getMaxStreamsUni());
    }
    if (properties.getMaxData() != null) {
      builder.maxData(properties.getMaxData());
    }
    if (properties.getCapsuleMaxLength() != null) {
      builder.capsuleMaxLength(properties.getCapsuleMaxLength());
    }
    if (!properties.getAllowedOrigins().isEmpty()) {
      builder.allowedOrigins(properties.getAllowedOrigins());
    }

    metricsListenerProvider.ifAvailable(builder::metricsListener);
    tokenHandlerProvider.ifAvailable(builder::quicTokenHandler);

    // Auto-discover @WebTransportEndpoint annotated beans or WebTransportHandler beans
    boolean hasExplicitDefault = false;
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
            if (hasExplicitDefault) {
              throw new IllegalStateException(
                  "Multiple @WebTransportEndpoint(isDefault = true) beans found");
            }
            hasExplicitDefault = true;
            builder.defaultHandler(handler);
          } else {
            builder.handler(path, handler);
          }
        } else if (bean instanceof ReactiveWebTransportHandler) {
          ReactiveWebTransportHandler reactiveHandler = (ReactiveWebTransportHandler) bean;
          if (isDefault) {
            if (hasExplicitDefault) {
              throw new IllegalStateException(
                  "Multiple @WebTransportEndpoint(isDefault = true) beans found");
            }
            hasExplicitDefault = true;
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

    // Also discover plain WebTransportHandler / ReactiveWebTransportHandler beans that are not
    // annotated if no explicit default endpoint annotation is present
    Map<String, WebTransportHandler> handlers =
        applicationContext.getBeansOfType(WebTransportHandler.class);
    it.unimi.dsi.fastutil.objects.ObjectArrayList<Map.Entry<String, WebTransportHandler>>
        unannotatedHandlers = new it.unimi.dsi.fastutil.objects.ObjectArrayList<>();
    for (Map.Entry<String, WebTransportHandler> entry : handlers.entrySet()) {
      if (!endpointBeans.containsKey(entry.getKey())) {
        unannotatedHandlers.add(entry);
      }
    }

    Map<String, ReactiveWebTransportHandler> reactiveHandlers =
        applicationContext.getBeansOfType(ReactiveWebTransportHandler.class);
    it.unimi.dsi.fastutil.objects.ObjectArrayList<Map.Entry<String, ReactiveWebTransportHandler>>
        unannotatedReactiveHandlers = new it.unimi.dsi.fastutil.objects.ObjectArrayList<>();
    for (Map.Entry<String, ReactiveWebTransportHandler> entry : reactiveHandlers.entrySet()) {
      if (!endpointBeans.containsKey(entry.getKey())) {
        unannotatedReactiveHandlers.add(entry);
      }
    }

    int totalUnannotated = unannotatedHandlers.size() + unannotatedReactiveHandlers.size();
    if (totalUnannotated > 1) {
      it.unimi.dsi.fastutil.objects.ObjectArrayList<String> names =
          new it.unimi.dsi.fastutil.objects.ObjectArrayList<>(totalUnannotated);
      for (Map.Entry<String, WebTransportHandler> entry : unannotatedHandlers) {
        names.add(entry.getKey());
      }
      for (Map.Entry<String, ReactiveWebTransportHandler> entry : unannotatedReactiveHandlers) {
        names.add(entry.getKey());
      }
      throw new IllegalStateException(
          "Multiple unannotated WebTransportHandler beans found: "
              + String.join(", ", names)
              + ". Annotate each handler bean with @WebTransportEndpoint(path = ...) or configure"
              + " an explicit default handler.");
    } else if (!hasExplicitDefault && totalUnannotated == 1) {
      if (unannotatedHandlers.size() == 1) {
        Map.Entry<String, WebTransportHandler> single = unannotatedHandlers.get(0);
        logger.info(
            "📡 Discovered unannotated WebTransportHandler bean '{}', registering at default path"
                + " '/'",
            single.getKey());
        builder.defaultHandler(single.getValue());
      } else {
        Map.Entry<String, ReactiveWebTransportHandler> single =
            unannotatedReactiveHandlers.get(0);
        logger.info(
            "📡 Discovered unannotated ReactiveWebTransportHandler bean '{}', registering at"
                + " default path '/'",
            single.getKey());
        builder.defaultReactiveHandler(single.getValue());
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
  @ConditionalOnMissingBean
  public SpringWebTransportServerLifecycle springWebTransportServerLifecycle(
      WebTransportServer server) {
    return new SpringWebTransportServerLifecycle(server);
  }
}

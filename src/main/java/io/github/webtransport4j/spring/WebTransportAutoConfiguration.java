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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Auto-Configuration for WebTransport4J.
 * Discovers handlers, endpoints annotated with {@link WebTransportEndpoint},
 * custom metric listeners, executors, and bootstraps {@link WebTransportServer}.
 */
@Configuration
public class WebTransportAutoConfiguration implements ApplicationContextAware {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportAutoConfiguration.class);

  private ApplicationContext applicationContext;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Bean
  public WebTransportProperties webTransportProperties() {
    return new WebTransportProperties();
  }

  @Bean
  public WebTransportServer webTransportServer(
      WebTransportProperties properties,
      ObjectProvider<WebTransportMetricsListener> metricsListenerProvider,
      ObjectProvider<QuicTokenHandler> tokenHandlerProvider,
      ObjectProvider<ExecutorService> executorProvider) {

    WebTransportServerBuilder builder = WebTransportServer.builder();

    builder.port(properties.getPort())
        .sslKeyPath(properties.getSslKeyPath())
        .sslCertPath(properties.getSslCertPath())
        .transportType(properties.getTransport())
        .idleTimeout(properties.getIdleTimeoutSeconds(), java.util.concurrent.TimeUnit.SECONDS)
        .maxStreams(properties.getMaxStreamsBidi(), properties.getMaxStreamsUni())
        .maxData(properties.getMaxData());

    if (!properties.getAllowedOrigins().isEmpty()) {
      builder.allowedOrigins(properties.getAllowedOrigins());
    }

    metricsListenerProvider.ifAvailable(builder::metricsListener);
    tokenHandlerProvider.ifAvailable(builder::quicTokenHandler);
    executorProvider.ifAvailable(builder::businessExecutor);

    // Auto-discover @WebTransportEndpoint annotated beans or WebTransportHandler beans
    Map<String, Object> endpointBeans = applicationContext.getBeansWithAnnotation(WebTransportEndpoint.class);
    for (Map.Entry<String, Object> entry : endpointBeans.entrySet()) {
      Object bean = entry.getValue();
      WebTransportEndpoint ann = bean.getClass().getAnnotation(WebTransportEndpoint.class);
      if (ann == null) {
        // Class level annotation might be on target class if CGLIB proxy
        ann = org.springframework.core.annotation.AnnotationUtils.findAnnotation(bean.getClass(), WebTransportEndpoint.class);
      }
      if (ann != null) {
        String path = ann.path();
        boolean isDefault = ann.isDefault();
        logger.info("📡 Discovered Spring WebTransport Endpoint: path='{}', default={}, bean='{}'", path, isDefault, entry.getKey());

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
          logger.warn("⚠️ Bean '{}' is annotated with @WebTransportEndpoint but does not implement WebTransportHandler or ReactiveWebTransportHandler", entry.getKey());
        }
      }
    }

    // Also discover plain WebTransportHandler beans that are not annotated if no endpoint annotation present
    Map<String, WebTransportHandler> handlers = applicationContext.getBeansOfType(WebTransportHandler.class);
    for (Map.Entry<String, WebTransportHandler> entry : handlers.entrySet()) {
      WebTransportHandler handler = entry.getValue();
      if (!endpointBeans.containsKey(entry.getKey())) {
        logger.info("📡 Discovered unannotated WebTransportHandler bean '{}', registering at default path '/'", entry.getKey());
        builder.defaultHandler(handler);
      }
    }

    return builder.build();
  }

  @Bean
  public SpringWebTransportServerLifecycle springWebTransportServerLifecycle(WebTransportServer server) {
    return new SpringWebTransportServerLifecycle(server);
  }
}

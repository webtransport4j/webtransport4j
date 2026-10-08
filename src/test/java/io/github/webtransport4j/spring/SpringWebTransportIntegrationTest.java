package io.github.webtransport4j.spring;

import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.server.WebTransportServer;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

/** Integration test for Spring Boot auto-configuration and lifecycle. */
public class SpringWebTransportIntegrationTest {

  /** Sample endpoint component for Spring test. */
  @WebTransportEndpoint(path = "/spring-chat")
  @Component
  public static class ChatEndpoint implements WebTransportHandler {}

  @Test
  public void testSpringAutoConfigurationAndLifecycle() {
    System.setProperty("webtransport4j.server.port", "0");
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.register(WebTransportAutoConfiguration.class, ChatEndpoint.class);
    context.refresh();

    WebTransportServer server = context.getBean(WebTransportServer.class);
    Assert.assertNotNull(server);

    SpringWebTransportServerLifecycle lifecycle =
        context.getBean(SpringWebTransportServerLifecycle.class);
    Assert.assertNotNull(lifecycle);
    Assert.assertTrue(lifecycle.isAutoStartup());

    WebTransportHandler chatHandler = server.getHandler("/spring-chat");
    Assert.assertNotNull(chatHandler);
    Assert.assertTrue(chatHandler instanceof ChatEndpoint);

    context.close();
  }

  @Test
  public void testSpringEnvironmentPropertyBinding() {
    java.util.Map<String, Object> props = new java.util.HashMap<>();
    props.put("webtransport4j.port", 0);
    props.put("webtransport4j.transport", "nio");
    props.put("webtransport4j.idle-timeout-seconds", 120);
    props.put("webtransport4j.max-streams-bidi", 250);
    props.put("webtransport4j.max-streams-uni", 350);
    props.put("webtransport4j.max-data", 52428800L);
    props.put("webtransport4j.capsule-max-length", 32768);
    props.put("webtransport4j.allowed-origins", "https://app1.internal,https://app2.internal");

    final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context
        .getEnvironment()
        .getPropertySources()
        .addFirst(new org.springframework.core.env.MapPropertySource("testProps", props));
    context.register(WebTransportAutoConfiguration.class, ChatEndpoint.class);
    context.refresh();

    WebTransportProperties properties = context.getBean(WebTransportProperties.class);
    Assert.assertNotNull(properties);
    Assert.assertEquals(0, properties.getPort());
    Assert.assertEquals("nio", properties.getTransport());
    Assert.assertEquals(120, properties.getIdleTimeoutSeconds());
    Assert.assertEquals(250, properties.getMaxStreamsBidi());
    Assert.assertEquals(350, properties.getMaxStreamsUni());
    Assert.assertEquals(52428800L, properties.getMaxData());
    Assert.assertEquals(32768, properties.getCapsuleMaxLength());
    Assert.assertEquals(2, properties.getAllowedOrigins().size());
    Assert.assertTrue(properties.getAllowedOrigins().contains("https://app1.internal"));
    Assert.assertTrue(properties.getAllowedOrigins().contains("https://app2.internal"));

    context.close();
  }
}

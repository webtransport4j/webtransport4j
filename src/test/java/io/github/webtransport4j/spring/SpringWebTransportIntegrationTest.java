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
}

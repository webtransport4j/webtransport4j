package io.github.webtransport4j.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to declare a class as a WebTransport handler endpoint for a specific path.
 * Compatible with Spring Boot, Quarkus, Micronaut, and plain CDI dependency injection.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface WebTransportEndpoint {

  /**
   * The URI path on which this WebTransport handler should be registered (e.g., "/chat", "/events").
   * Defaults to "/".
   *
   * @return the URI path for the endpoint.
   */
  String path() default "/";

  /**
   * Whether this handler should serve as the default fallback handler if no exact path matches.
   *
   * @return {@code true} if this handler is the default handler.
   */
  boolean isDefault() default false;
}

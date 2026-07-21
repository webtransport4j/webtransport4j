# WebTransport4J - Complete API Documentation & Practical Guide

Welcome to **WebTransport4J**, a high-performance, production-ready WebTransport (HTTP/3 over QUIC) library for Java.

This documentation covers the core API, async programming model with `CompletableFuture`, declarative endpoint mapping (`@WebTransportEndpoint`), and complete, copy-pasteable practical application examples for:
- **Plain Java (Standalone / Embedded)**
- **Spring Boot Applications**
- **Quarkus Microservices**

---

## Table of Contents
1. [Core Concepts & Async Model](#core-concepts--async-model)
2. [Standalone / Plain Java Examples](#1-standalone--plain-java-application)
   - [Basic Echo Server](#a-basic-echo-server)
   - [Reactive Chat Endpoint](#b-reactive-chat-endpoint)
3. [Spring Boot Integration](#2-spring-boot-integration)
   - [Configuration Properties](#a-spring-boot-configuration-properties)
   - [Declarative Spring Endpoints](#b-declarative-spring-endpoints)
4. [Quarkus Integration](#3-quarkus-integration)
   - [CDI Endpoint Beans](#a-cdi-endpoint-beans)
   - [Quarkus Lifecycle Setup](#b-quarkus-lifecycle-setup)
5. [Session & Stream API Reference](#4-session--stream-api-reference)
   - [WebTransportSession](#webtransportsession)
   - [WebTransportStream](#webtransportstream)
   - [Datagram Transmission](#datagram-transmission)
6. [Production Readiness & Tuning](#5-production-readiness--tuning)

---

## Core Concepts & Async Model

- **Non-Blocking Architecture**: WebTransport4J uses Netty QUIC & HTTP/3 event loops under the hood with non-blocking server lifecycle. `server.start()` binds the UDP port and returns immediately.
- **CompletableFuture Async API**: All public asynchronous methods (`createUniStream()`, `createBiStream()`, `write()`, `writeText()`, `shutdown()`) return JDK standard `CompletableFuture<T>`. This allows effortless integration with Java 21+ Virtual Threads, Spring WebFlux (`Mono.fromFuture`), and Quarkus Mutiny (`Uni.createFrom().completionStage`).
- **Path-Based Routing**: Register different handlers for URI endpoints (e.g., `/chat`, `/telemetry`, `/video-stream`).

---

## 1. Standalone / Plain Java Application

### A. Basic Echo Server

A complete runnable standalone main application using `WebTransportServer.builder()`:

```java
package io.github.webtransport4j.example;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.server.WebTransportServer;
import java.io.File;

public class StandaloneWebTransportApp {

  public static void main(String[] args) throws Exception {
    WebTransportServer server = WebTransportServer.builder()
        .port(4433)
        .ssl("localhost-key.pem", "localhost.pem")
        .allowedOrigins("https://example.com", "https://localhost:3000")
        .transportType("auto") // Auto-selects Epoll, KQueue, IOUring, or NIO
        .handler("/echo", new WebTransportHandler() {
          
          @Override
          public void onSessionReady(WebTransportSession session) {
            System.out.println("🟢 Client connected to session: " + session.getSessionStreamId());
          }

          @Override
          public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
            System.out.println("📥 Stream opened: " + stream.streamId());
            
            // Listen for data on incoming stream and echo back
            stream.onData(buffer -> {
              byte[] bytes = new byte[buffer.readableBytes()];
              buffer.readBytes(bytes);
              String text = new String(bytes);
              System.out.println("Received: " + text);

              // Send response back using CompletableFuture API
              if (stream.isBidirectional()) {
                stream.writeText("Echo: " + text)
                    .thenRun(() -> System.out.println("✅ Echoed back successfully"))
                    .exceptionally(ex -> {
                      System.err.println("❌ Write failed: " + ex.getMessage());
                      return null;
                    });
              }
            });
          }

          @Override
          public void onDatagramReceived(WebTransportSession session, WebTransportBuffer data) {
            byte[] payload = new byte[data.readableBytes()];
            data.readBytes(payload);
            System.out.println("☄️ Received Datagram of length: " + payload.length);
            
            // Echo datagram back
            session.sendDatagram(payload);
          }
        })
        .build();

    // Start server non-blockingly and block main thread
    server.startAndAwait();
  }
}
```

### B. Reactive Chat Endpoint

For reactive pipelines using `ReactiveWebTransportHandler`:

```java
package io.github.webtransport4j.example;

import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.ReactiveWebTransportSession;
import io.github.webtransport4j.api.ReactiveWebTransportStream;
import io.github.webtransport4j.api.EmptyPublisher;
import io.github.webtransport4j.server.WebTransportServer;
import org.reactivestreams.Publisher;

public class ReactiveChatApp {

  public static void main(String[] args) throws Exception {
    WebTransportServer server = WebTransportServer.builder()
        .port(4433)
        .ssl("localhost-key.pem", "localhost.pem")
        .reactiveHandler("/chat", new ReactiveWebTransportHandler() {

          @Override
          public Publisher<Void> onSessionReady(ReactiveWebTransportSession session) {
            System.out.println("🟢 Reactive session ready: " + session.path());
            return EmptyPublisher.instance();
          }

          @Override
          public Publisher<Void> onIncomingStream(
              ReactiveWebTransportSession session, ReactiveWebTransportStream stream) {
            stream.incomingData().subscribe(buffer -> {
              System.out.println("Received reactive buffer size: " + buffer.readableBytes());
            });
            return EmptyPublisher.instance();
          }
        })
        .build();

    server.start();
    System.out.println("Server listening on port " + server.getPort());
  }
}
```

---

## 2. Spring Boot Integration

WebTransport4J provides native Spring Boot Auto-Configuration with `@WebTransportEndpoint` bean scanning and `SmartLifecycle` management.

### A. Spring Boot `application.properties`

Add configuration settings to `src/main/resources/application.properties` or `application.yml`:

```properties
# WebTransport4J Settings
webtransport4j.port=4433
webtransport4j.ssl-key-path=/etc/ssl/localhost-key.pem
webtransport4j.ssl-cert-path=/etc/ssl/localhost.pem
webtransport4j.allowed-origins=https://my-app.com,https://localhost:3000
webtransport4j.transport=auto
webtransport4j.idle-timeout-seconds=60
webtransport4j.max-streams-bidi=500
webtransport4j.max-streams-uni=500
```

### B. Declarative Spring Endpoints

Annotate your Spring `@Component` or `@Service` classes with `@WebTransportEndpoint`:

```java
package com.example.demo.webtransport;

import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import org.springframework.stereotype.Component;

@WebTransportEndpoint(path = "/live-metrics")
@Component
public class LiveMetricsEndpoint implements WebTransportHandler {

  @Override
  public void onSessionReady(WebTransportSession session) {
    System.out.println("🌱 Spring Boot WebTransport Client Connected: " + session.path());
    
    // Periodically create server-initiated stream to push metrics to client
    session.createBiStream()
        .thenAccept(stream -> {
          stream.writeText("Welcome to Live Metrics Stream!")
              .thenRun(() -> System.out.println("Sent welcome message"));
        });
  }

  @Override
  public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
    stream.onData(buffer -> {
      byte[] data = new byte[buffer.readableBytes()];
      buffer.readBytes(data);
      System.out.println("Metrics query received: " + new String(data));
      stream.writeText("Metrics Response: OK");
    });
  }
}
```

#### Main Spring Boot Application:

```java
package com.example.demo;

import io.github.webtransport4j.spring.WebTransportAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(WebTransportAutoConfiguration.class)
public class DemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(DemoApplication.class, args);
  }
}
```

> **Note**: `WebTransportAutoConfiguration` automatically starts the server via `SmartLifecycle` on Spring application context startup and shuts down gracefully on application termination.

---

## 3. Quarkus Integration

WebTransport4J supports Quarkus applications using CDI bean discovery and lifecycle event observers.

### A. CDI Endpoint Beans

Annotate your Quarkus CDI beans (`@ApplicationScoped`, `@Singleton`) with `@WebTransportEndpoint`:

```java
package com.example.quarkus;

import io.github.webtransport4j.api.WebTransportEndpoint;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import jakarta.enterprise.context.ApplicationScoped;

@WebTransportEndpoint(path = "/events")
@ApplicationScoped
public class EventStreamEndpoint implements WebTransportHandler {

  @Override
  public void onSessionReady(WebTransportSession session) {
    System.out.println("⚡ Quarkus WebTransport Session Connected");
  }

  @Override
  public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
    stream.onData(buf -> {
      byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      stream.writeText("Quarkus Received: " + new String(bytes));
    });
  }
}
```

### B. Quarkus Lifecycle Setup

Hook into Quarkus startup and shutdown events using `QuarkusWebTransportManager`:

```java
package com.example.quarkus;

import io.github.webtransport4j.quarkus.QuarkusWebTransportManager;
import io.github.webtransport4j.server.WebTransportServer;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

@ApplicationScoped
public class WebTransportLifecycleBean {

  private QuarkusWebTransportManager manager;

  void onStartup(@Observes StartupEvent ev, @Any Instance<Object> cdiBeans) {
    // Discover all CDI beans annotated with @WebTransportEndpoint
    java.util.List<Object> beans = new java.util.ArrayList<>();
    cdiBeans.forEach(beans::add);

    manager = QuarkusWebTransportManager.create(
        WebTransportServer.builder()
            .port(4433)
            .ssl("localhost-key.pem", "localhost.pem"),
        beans
    );

    // Non-blocking start
    manager.onStartup();
  }

  void onShutdown(@Observes ShutdownEvent ev) {
    if (manager != null) {
      manager.onShutdown();
    }
  }
}
```

---

## 4. Session & Stream API Reference

### WebTransportSession

The `WebTransportSession` manages connection lifecycle, settings, and server-initiated streams:

```java
// Server-initiated unidirectional stream
session.createUniStream()
    .thenAccept(stream -> {
      stream.writeText("Push Notification");
      stream.close();
    });

// Server-initiated bidirectional stream
session.createBiStream()
    .thenAccept(stream -> {
      stream.writeText("Hello from Server");
      stream.onData(buf -> System.out.println("Client replied"));
    });

// Session attributes
String path = session.path();
long id = session.getSessionStreamId();
String token = session.getResumptionToken();

// Graceful close or abort with HTTP/3 error code
session.close();
session.abort(0x00);
```

### WebTransportStream

`WebTransportStream` provides high-performance zero-copy write operations returning `CompletableFuture<Void>`:

```java
// String write
CompletableFuture<Void> f1 = stream.writeText("Hello World");

// Byte array write
byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
CompletableFuture<Void> f2 = stream.write(data);

// ByteBuffer write
ByteBuffer nioBuf = ByteBuffer.wrap(data);
CompletableFuture<Void> f3 = stream.write(nioBuf);

// BinarySource streaming (files, channels, byte arrays)
Path filePath = Paths.get("/var/data/largefile.bin");
BinarySource fileSource = BinarySources.fromPath(filePath);
CompletableFuture<Void> f4 = stream.write(fileSource, 65536); // 64KB chunk size

// Stream lifecycle & attributes
stream.setAttribute("userId", "usr_123");
String userId = stream.getAttribute("userId", String.class);
stream.close();
stream.reset(0x01); // Reset stream with error code
```

### Datagram Transmission

WebTransport Datagrams are un-ordered, un-reliable UDP datagrams:

```java
// Send raw byte array
byte[] payload = new byte[]{0x01, 0x02, 0x03};
session.sendDatagram(payload);

// Receive datagram in handler
@Override
public void onDatagramReceived(WebTransportSession session, WebTransportBuffer data) {
  byte[] received = new byte[data.readableBytes()];
  data.readBytes(received);
}
```

---

## 5. Production Readiness & Tuning

1. **Native Transports**:
   WebTransport4J automatically detects and utilizes OS-native transport epoll (Linux), kqueue (macOS), or IOUring (Linux 5.1+):
   ```java
   builder.transportType("auto"); // or "epoll", "kqueue", "iouring", "nio"
   ```

2. **1-RTT Session Resumption**:
   Provide session ticket keys to enable 1-RTT TLS session resumption across cluster nodes:
   ```properties
   webtransport4j.ssl.session.ticket.keys=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
   ```

3. **Virtual Threads Support**:
   For Java 21+, virtual threads are automatically utilized for non-blocking handler callbacks when enabled via system property or configured business executor.

4. **Metrics & Observability**:
   Attach custom Micrometer / OpenTelemetry / Datadog listeners:
   ```java
   builder.metricsListener(new CustomMetricsListener());
   ```

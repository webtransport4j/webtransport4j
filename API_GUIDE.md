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
   - [Outbound Backpressure & Flow Control](#outbound-backpressure--flow-control)
   - [Stream Priority & Incremental Scheduling (RFC 9218)](#stream-priority--incremental-scheduling-rfc-9218)
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

// Server-initiated streams with RFC 9218 priority
session.createBiStream(StreamPriority.HIGHEST) // urgency = 0
    .thenAccept(controlStream -> {
      controlStream.writeText("Urgent Control Signal");
    });

session.createUniStream(StreamPriority.of(5, true)) // urgency = 5, incremental/interleaved = true
    .thenAccept(mediaStream -> {
      mediaStream.write(videoChunk);
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

// Stream Priority (RFC 9218) - dynamic updates
stream.setPriority(StreamPriority.HIGHEST);
stream.setPriority(1, true); // urgency 1, incremental interleaving
StreamPriority currentPriority = stream.getPriority();

// Stream lifecycle & attributes
stream.setAttribute("userId", "usr_123");
String userId = stream.getAttribute("userId", String.class);
stream.close();
stream.reset(0x01); // Reset stream with error code
```

### Sending Data (Text, JSON, Files)

The `WebTransportStream` API provides convenient, zero-copy methods for sending various data types. It is designed to be highly performant across all Java versions (Java 8 through Java 25).

#### 1. Sending Normal Text & JSON
You can send plain text or JSON payloads easily. Since JSON is naturally UTF-8 text, `writeText()` is the recommended method.

```java
// Small text payloads (Non-blocking, no writability check needed)
stream.writeText("Welcome to the server!");

// JSON payload (String)
String jsonString = "{\"action\": \"LOGIN\", \"status\": \"SUCCESS\"}";
stream.writeText(jsonString);
```
> **Warning**: For small, infrequent text messages, you can safely call `writeText()` without checking backpressure. However, if you are rapidly streaming thousands of JSON objects, **you must check writability** (see Backpressure section) or you will cause an `OutOfMemoryError`.

For maximum performance (e.g., when using Jackson or Gson), serialize your object directly to a byte array to avoid intermediate String allocations:
```java
byte[] jsonBytes = objectMapper.writeValueAsBytes(myObject);
stream.write(jsonBytes);
```

#### 2. Sending Big Files (with built-in chunking)
For large files or streams, WebTransport4J provides `BinarySource`, which automatically chunks the file and streams it over the network asynchronously without loading the whole file into RAM.

```java
Path filePath = Paths.get("/var/data/large_video.mp4");
BinarySource source = BinarySources.fromPath(filePath);

// Streams the file in 64KB chunks automatically. 
// No manual backpressure handling required here, as BinarySource handles it internally!
stream.write(source, 65536)
      .thenRun(() -> System.out.println("File transfer complete!"));
```


### Outbound Backpressure & Flow Control

#### The Risk of Unbounded Buffering
Naively looping over `stream.write(...)` without awaiting or chaining the returned `CompletableFuture` causes writes to accumulate in Netty's `ChannelOutboundBuffer`. Under network congestion, this leads to memory exhaustion and `OutOfMemoryError`.

```java
// ❌ ANTI-PATTERN: Naive unawaited loop causes OOM under congestion
for (byte[] chunk : largeDataSet) {
  stream.write(chunk); // Writes enqueue faster than network can drain!
}
```

#### How WebTransport4J Handles This (Netty-Native)
WebTransport4J leverages Netty's built-in `ChannelOutboundBuffer` watermarks — **no redundant application-layer queue**:

1. **`WriteBufferWaterMark`** is configured on every `QuicStreamChannel` (default: 2MB low / 4MB high). When pending bytes in `ChannelOutboundBuffer` exceed the high watermark, `channel.isWritable()` flips to `false`. When drained below the low watermark, it flips back to `true`.
2. **`stream.isWritable()`** delegates directly to `channel.isWritable()` — applications can check this before writing.
3. **`stream.waitForWritable()`** returns a `CompletableFuture<Void>` that completes when Netty's `channelWritabilityChanged` fires with `writable=true`.
4. **`stream.onWritabilityChanged(listener)`** provides an event-driven callback bridging Netty's pipeline to the stream API.

#### Backpressure API

```java
// 1. Check writability (delegates to Netty's ChannelOutboundBuffer watermarks)
boolean writable = stream.isWritable();

// 2. Await writability asynchronously (completes immediately if already writable)
CompletableFuture<Void> writableFuture = stream.waitForWritable();

// 3. Event-driven listener
stream.onWritabilityChanged(isWritable -> {
  if (isWritable) {
    System.out.println("Stream ready for more writes");
  } else {
    System.out.println("Stream congested; pause producing data");
  }
});
```

#### Production Patterns

Click the section below that matches your stack and Java version:

<details>
  <summary><strong>Java 21+ (Virtual Threads) - Recommended</strong></summary>

With Java 21 Virtual Threads, you can safely `.join()` futures without blocking expensive OS carrier threads. This creates the most readable code.

```java
while (dataSource.hasNext()) {
  if (!stream.isWritable()) {
    // Yields the virtual thread, does not block OS thread!
    stream.waitForWritable().join();
  }
  stream.write(dataSource.next()).join();
}
```
</details>

<details>
  <summary><strong>Java 8+ (Asynchronous Future Chaining)</strong></summary>

If you cannot use Virtual Threads, use `.thenCompose(...)` to chain writes. This ensures subsequent chunks are only dispatched after preceding writes hit the network.

```java
CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
for (byte[] chunk : chunks) {
  chain = chain.thenCompose(v -> stream.write(chunk));
}

// Or as a recursive loop checking writability:
public CompletableFuture<Void> sendAsync(Iterator<byte[]> it, WebTransportStream stream) {
  if (!it.hasNext()) return CompletableFuture.completedFuture(null);
  
  if (!stream.isWritable()) {
    return stream.waitForWritable().thenCompose(v -> sendAsync(it, stream));
  }
  return stream.write(it.next()).thenCompose(v -> sendAsync(it, stream));
}
```
</details>

<details>
  <summary><strong>Reactive Streams (Spring WebFlux / Project Reactor)</strong></summary>

For Reactive applications, `ReactiveWebTransportStream` is a full `Subscriber` that naturally paces itself using reactive backpressure (request(1)).

```java
ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(stream);

// The Subscriber automatically pulls data only when the stream is writable!
Flux.fromIterable(largeDataSet)
    .map(WebTransportBuffer::wrap)
    .subscribe(reactiveStream);
```
</details>

#### Configuration

| Property | Default | Description |
| :--- | :--- | :--- |
| `webtransport4j.netty.write_buffer.high_water_mark` | `4194304` (4 MB) | Netty `ChannelOutboundBuffer` high watermark per stream. `isWritable()` → `false` when exceeded. |
| `webtransport4j.netty.write_buffer.low_water_mark` | `2097152` (2 MB) | Netty low watermark. `isWritable()` → `true` when drained below. |

### Stream Priority & Incremental Scheduling (RFC 9218)

WebTransport4J implements the IETF RFC 9218 (*Extensible Prioritization Scheme for HTTP/QUIC*), providing fine-grained control over bandwidth allocation and stream scheduling:

#### Priority Parameters
- **`urgency` (0–7)**:
  - `0` is the highest urgency (critical sync signals, urgent control frames).
  - `3` is the default urgency per RFC 9218.
  - `7` is the lowest urgency (background telemetry, prefetching).
- **`incremental` (boolean)**:
  - `false` (default): sequential / exclusive scheduling. Frames from this stream are dispatched without interleaving among sibling streams of equal urgency.
  - `true`: incremental / round-robin scheduling. Frames from equal-urgency streams are interleaved concurrently, preventing head-of-line blocking across media chunks, progressive assets, or concurrent downloads.

#### Priority API Usage

```java
import io.github.webtransport4j.api.StreamPriority;

// Predefined constants
StreamPriority defaultPri = StreamPriority.DEFAULT; // urgency=3, incremental=false
StreamPriority highestPri = StreamPriority.HIGHEST; // urgency=0, incremental=false
StreamPriority lowestPri  = StreamPriority.LOWEST;  // urgency=7, incremental=false

// Factory methods
StreamPriority custom     = StreamPriority.of(2, true);
StreamPriority urgentOnly = StreamPriority.urgency(1);       // default incremental (false)
StreamPriority incOnly    = StreamPriority.incremental(true); // default urgency (3)

// Fluent copies
StreamPriority modified = custom.withUrgency(0).withIncremental(false);

// Querying priority
int urgency   = custom.urgency();       // or custom.getUrgency()
boolean isInc = custom.isIncremental(); // or custom.incremental()

// 1. Setting priority at stream creation:
session.createBiStream(StreamPriority.of(1, true))
    .thenAccept(stream -> {
      // Stream is scheduled with urgency=1 and incremental interleaving
    });

// 2. Updating priority dynamically on an existing stream:
stream.setPriority(StreamPriority.HIGHEST)
    .thenRun(() -> System.out.println("Priority promoted to HIGHEST"));

// 3. Reactive Streams support:
reactiveSession.createBiStream(StreamPriority.of(2, true))
    .subscribe(subscriber);

reactiveStream.setPriority(StreamPriority.HIGHEST);
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

---

## ⚠️ Critical Production Deployment Warnings

> [!WARNING]
> **1. Production TLS Certificates**
> Never rely on developer fallback paths or in-memory self-signed certificates in production deployments. Always specify valid CA-signed certificate paths (e.g., Let's Encrypt) via system properties or configuration:
> ```properties
> webtransport4j.ssl.cert.path=/etc/letsencrypt/live/yourdomain.com/fullchain.pem
> webtransport4j.ssl.key.path=/etc/letsencrypt/live/yourdomain.com/privkey.pem
> ```

> [!IMPORTANT]
> **2. Linux OS Socket Buffer & File Descriptor Tuning**
> Operating at scale ($10,000+$ concurrent sessions) on Linux requires raising file descriptor limits and kernel UDP socket buffer sizes to prevent OS-level packet drops:
> ```bash
> # File descriptor limit:
> ulimit -n 1048576
> 
> # Expand Linux Kernel UDP Socket Buffer Limits (16 MB):
> sudo sysctl -w net.core.rmem_max=16777216
> sudo sysctl -w net.core.wmem_max=16777216
> sudo sysctl -w net.core.rmem_default=8388608
> sudo sysctl -w net.core.wmem_default=8388608
> ```


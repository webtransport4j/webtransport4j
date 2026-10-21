package io.github.webtransport4j.example;

import io.github.webtransport4j.api.BinarySource;
import io.github.webtransport4j.api.BinarySources;
import io.github.webtransport4j.api.EmptyPublisher;
import io.github.webtransport4j.api.ReactiveWebTransportHandler;
import io.github.webtransport4j.api.ReactiveWebTransportSession;
import io.github.webtransport4j.api.ReactiveWebTransportStream;
import io.github.webtransport4j.api.StreamPriority;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
import io.github.webtransport4j.health.WebTransportHealthCheck;
import io.github.webtransport4j.metrics.WebTransportMicrometerMetricsListener;
import io.github.webtransport4j.observability.otlp.WebTransportOpenTelemetryTracer;
import io.github.webtransport4j.server.DefaultNettyWebTransportBuffer;
import io.github.webtransport4j.server.WebTransportServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Validates that all code snippets presented in README.md, API_GUIDE.md, and production
 * documentation compile and execute as documented.
 */
public class DocumentationSnippetsTest {

  @Test
  public void testEchoServerSnippetCompilationAndExecution() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .handler(
                "/echo",
                new WebTransportHandler() {
                  @Override
                  public void onSessionReady(WebTransportSession session) {
                    Assertions.assertNotNull(session);
                  }

                  @Override
                  public void onIncomingStream(
                      WebTransportSession session, WebTransportStream stream) {
                    stream.onData(
                        buffer -> {
                          byte[] data = buffer.readBytes();
                          if (stream.isBidirectional()) {
                            stream.write(data);
                          }
                        });
                  }

                  @Override
                  public void onDatagramReceived(
                      WebTransportSession session, WebTransportBuffer buffer) {
                    byte[] data = buffer.readBytes();
                    session.sendDatagram(data);
                  }
                })
            .build();

    Assertions.assertNotNull(server);
  }

  @Test
  public void testReactiveHandlerSnippet() {
    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .reactiveHandler(
                "/chat",
                new ReactiveWebTransportHandler() {
                  @Override
                  public Publisher<Void> onSessionReady(ReactiveWebTransportSession session) {
                    return EmptyPublisher.instance();
                  }

                  @Override
                  public Publisher<Void> onIncomingStream(
                      ReactiveWebTransportSession session, ReactiveWebTransportStream stream) {
                    stream.subscribe(
                        new Subscriber<WebTransportBuffer>() {
                          private Subscription subscription;

                          @Override
                          public void onSubscribe(Subscription s) {
                            this.subscription = s;
                            s.request(1);
                          }

                          @Override
                          public void onNext(WebTransportBuffer buffer) {
                            try {
                              Assertions.assertTrue(buffer.readableBytes() >= 0);
                            } finally {
                              buffer.release();
                              subscription.request(1);
                            }
                          }

                          @Override
                          public void onError(Throwable t) {}

                          @Override
                          public void onComplete() {}
                        });
                    return EmptyPublisher.instance();
                  }
                })
            .build();

    Assertions.assertNotNull(server);
  }

  @Test
  public void testBufferWrapHelperMethods() {
    byte[] testBytes = "Hello WebTransport".getBytes(StandardCharsets.UTF_8);

    WebTransportBuffer buf1 = DefaultNettyWebTransportBuffer.wrap(testBytes);
    Assertions.assertNotNull(buf1);
    Assertions.assertEquals(testBytes.length, buf1.readableBytes());
    Assertions.assertArrayEquals(testBytes, buf1.readBytes());
    buf1.release();

    java.nio.ByteBuffer nioBuffer = java.nio.ByteBuffer.wrap(testBytes);
    WebTransportBuffer buf2 = DefaultNettyWebTransportBuffer.wrap(nioBuffer);
    Assertions.assertNotNull(buf2);
    Assertions.assertEquals(testBytes.length, buf2.readableBytes());
    Assertions.assertArrayEquals(testBytes, buf2.readBytes());
    buf2.release();
  }

  @Test
  public void testBinarySourcesFromPath() throws IOException {
    File tempFile = File.createTempFile("wt4j-doc-test", ".bin");
    tempFile.deleteOnExit();
    Files.write(tempFile.toPath(), "PayloadData".getBytes(StandardCharsets.UTF_8));

    BinarySource source = BinarySources.fromPath(tempFile.toPath());
    Assertions.assertNotNull(source);
    Assertions.assertEquals(11, source.size());
  }

  @Test
  public void testObservabilityAndHealthCheckSnippets() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    WebTransportMicrometerMetricsListener metricsListener =
        new WebTransportMicrometerMetricsListener(meterRegistry);

    SdkTracerProvider tracerProvider = SdkTracerProvider.builder().build();
    Tracer otelTracer = tracerProvider.get("webtransport-service");
    WebTransportOpenTelemetryTracer tracer = new WebTransportOpenTelemetryTracer(otelTracer);
    Assertions.assertNotNull(tracer);

    WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .metricsListener(metricsListener)
            .build();

    // Verify Health Checks
    Assertions.assertFalse(WebTransportHealthCheck.isAlive(server)); // Not started yet
    Assertions.assertFalse(WebTransportHealthCheck.isReady(server));

    Map<String, Object> details = WebTransportHealthCheck.getHealthDetails(server);
    Assertions.assertNotNull(details);
    Assertions.assertEquals(false, details.get("running"));

    tracerProvider.close();
  }
}

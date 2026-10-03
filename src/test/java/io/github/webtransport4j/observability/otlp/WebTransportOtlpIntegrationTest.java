package io.github.webtransport4j.observability.otlp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.observability.WebTransportTraceContext;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.time.Duration;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Integration and unit tests for OTLP metrics and OpenTelemetry distributed tracing support. */
public class WebTransportOtlpIntegrationTest {

  private InMemorySpanExporter spanExporter;
  private SdkTracerProvider tracerProvider;
  private Tracer tracer;
  private WebTransportOpenTelemetryTracer wtTracer;

  /** Initializes OpenTelemetry SDK and in-memory span exporter for verification. */
  @Before
  public void setUp() {
    spanExporter = InMemorySpanExporter.create();
    tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    tracer = tracerProvider.get(WebTransportOpenTelemetryTracer.INSTRUMENTATION_NAME);
    wtTracer = new WebTransportOpenTelemetryTracer(tracer);
  }

  /** Shuts down OpenTelemetry tracer provider. */
  @After
  public void tearDown() {
    if (tracerProvider != null) {
      tracerProvider.close();
    }
  }

  @Test
  public void testOtlpConfigDefaultsAndCustomization() {
    WebTransportOtlpConfig defaultConfig = WebTransportOtlpConfig.builder().build();
    assertTrue(defaultConfig.enabled());
    assertEquals(WebTransportOtlpConfig.DEFAULT_URL, defaultConfig.url());
    assertEquals(Duration.ofSeconds(10), defaultConfig.step());
    assertEquals("webtransport4j", defaultConfig.resourceAttributes().get("service.name"));
    assertTrue(defaultConfig.headers().isEmpty());
    assertNull(defaultConfig.get("any.key"));

    WebTransportOtlpConfig customConfig =
        WebTransportOtlpConfig.builder()
            .enabled(false)
            .url("http://collector.prod:4318/v1/metrics")
            .step(Duration.ofSeconds(5))
            .header("Authorization", "Bearer token-123")
            .resourceAttribute("service.name", "my-gateway")
            .resourceAttribute("service.version", "2.0.0")
            .build();

    assertFalse(customConfig.enabled());
    assertEquals("http://collector.prod:4318/v1/metrics", customConfig.url());
    assertEquals(Duration.ofSeconds(5), customConfig.step());
    assertEquals("Bearer token-123", customConfig.headers().get("Authorization"));
    assertEquals("my-gateway", customConfig.resourceAttributes().get("service.name"));
    assertEquals("2.0.0", customConfig.resourceAttributes().get("service.version"));
  }

  @Test
  public void testOtlpMetricsListenerLifecycle() {
    WebTransportOtlpConfig config =
        WebTransportOtlpConfig.builder()
            .enabled(false)
            .url("http://localhost:4318/v1/metrics")
            .step(Duration.ofMinutes(1))
            .build();

    try (WebTransportOtlpMetricsListener listener = new WebTransportOtlpMetricsListener(config)) {
      assertNotNull(listener.getRegistry());

      listener.onSessionOpened(100L, "/live-feed");
      assertEquals(1, listener.getActiveSessions());

      listener.onStreamOpened(100L, 1L, true);
      listener.onStreamOpened(100L, 2L, false);
      assertEquals(2, listener.getActiveStreams());

      listener.onDatagramSent(100L, 1024);
      listener.onDatagramReceived(100L, 512);
      listener.onDatagramDiscarded(100L, "queue_full");

      listener.onStreamClosed(100L, 1L);
      assertEquals(1, listener.getActiveStreams());

      listener.onSessionClosed(100L, 0);
      assertEquals(0, listener.getActiveSessions());
    }
  }

  @Test
  public void testOpenTelemetryTracerSessionSpanSuccess() {
    String traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    WebTransportTraceContext parentCtx = WebTransportTraceContext.fromHeaders(traceparent, null);
    assertNotNull(parentCtx);

    Span span = wtTracer.startSessionSpan(42L, "/game-stream", parentCtx);
    assertNotNull(span);
    wtTracer.endSessionSpan(span, 0);

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData spanData = spans.get(0);
    assertEquals("WebTransport SESSION /game-stream", spanData.getName());
    assertEquals(SpanKind.SERVER, spanData.getKind());
    assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", spanData.getTraceId());
    assertEquals("00f067aa0ba902b7", spanData.getParentSpanId());
    assertEquals(StatusCode.OK, spanData.getStatus().getStatusCode());
    assertEquals(
        "webtransport", spanData.getAttributes().get(AttributeKey.stringKey("rpc.system")));
    assertEquals(
        Long.valueOf(42L),
        spanData.getAttributes().get(AttributeKey.longKey("webtransport.session_id")));
    assertEquals(
        Long.valueOf(0L),
        spanData.getAttributes().get(AttributeKey.longKey("webtransport.close_code")));
  }

  @Test
  public void testOpenTelemetryTracerSessionSpanError() {
    Span span = wtTracer.startSessionSpan(99L, "/faulty-stream", null);
    assertNotNull(span);
    wtTracer.endSessionSpan(span, 503);

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData spanData = spans.get(0);
    assertEquals(StatusCode.ERROR, spanData.getStatus().getStatusCode());
    assertEquals(
        Long.valueOf(503L),
        spanData.getAttributes().get(AttributeKey.longKey("webtransport.close_code")));
    assertFalse(spanData.getParentSpanContext().isValid());
  }

  @Test
  public void testInjectTraceContext() {
    Span span = wtTracer.startSessionSpan(77L, "/inject-test", null);
    WebTransportTraceContext injected = wtTracer.injectTraceContext(span);

    assertNotNull(injected);
    assertEquals(span.getSpanContext().getTraceId(), injected.getTraceId());
    assertEquals(span.getSpanContext().getSpanId(), injected.getSpanId());
    assertEquals(span.getSpanContext().getTraceFlags().asHex(), injected.getTraceFlags());

    span.end();
  }
}

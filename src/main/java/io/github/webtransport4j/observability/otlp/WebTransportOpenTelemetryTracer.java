package io.github.webtransport4j.observability.otlp;

import io.github.webtransport4j.observability.WebTransportTraceContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * OpenTelemetry distributed tracing bridge for WebTransport sessions and streams.
 */
public class WebTransportOpenTelemetryTracer {

  /** Instrumentation library name. */
  public static final String INSTRUMENTATION_NAME = "io.github.webtransport4j";

  private final Tracer tracer;

  /**
   * Constructs an OpenTelemetry tracer bridge.
   *
   * @param tracer OpenTelemetry tracer instance
   */
  public WebTransportOpenTelemetryTracer(@NonNull Tracer tracer) {
    this.tracer = Objects.requireNonNull(tracer, "tracer must not be null");
  }

  /**
   * Starts a new server-side OpenTelemetry span for an incoming WebTransport session.
   *
   * @param sessionId WebTransport session identifier
   * @param path request path
   * @param parentContext optional W3C trace context extracted from incoming headers
   * @return newly started {@link Span}
   */
  public @NonNull Span startSessionSpan(
      long sessionId,
      @NonNull String path,
      @Nullable WebTransportTraceContext parentContext) {
    Objects.requireNonNull(path, "path must not be null");

    final SpanBuilder spanBuilder =
        tracer
            .spanBuilder("WebTransport SESSION " + path)
            .setSpanKind(SpanKind.SERVER)
            .setAttribute("rpc.system", "webtransport")
            .setAttribute("webtransport.session_id", sessionId)
            .setAttribute("http.target", path);

    if (parentContext != null) {
      try {
        final TraceFlags flags = TraceFlags.fromHex(parentContext.getTraceFlags(), 0);
        final SpanContext remoteSpanContext =
            SpanContext.createFromRemoteParent(
                parentContext.getTraceId(),
                parentContext.getSpanId(),
                flags,
                TraceState.getDefault());
        spanBuilder.setParent(Context.current().with(Span.wrap(remoteSpanContext)));
      } catch (Exception ignored) {
        // Fall back to root span if parent context was malformed
      }
    }

    return spanBuilder.startSpan();
  }

  /**
   * Completes a WebTransport session span with termination status.
   *
   * @param span the active span to complete
   * @param closeCode WebTransport session termination status code
   */
  public void endSessionSpan(@NonNull Span span, int closeCode) {
    Objects.requireNonNull(span, "span must not be null");
    span.setAttribute("webtransport.close_code", (long) closeCode);
    if (closeCode == 0) {
      span.setStatus(StatusCode.OK);
    } else {
      span.setStatus(StatusCode.ERROR, "Session terminated with close code: " + closeCode);
    }
    span.end();
  }

  /**
   * Injects an active span's context into a {@link WebTransportTraceContext} for outgoing propagation.
   *
   * @param span active OpenTelemetry span
   * @return trace context representation
   */
  public @NonNull WebTransportTraceContext injectTraceContext(@NonNull Span span) {
    Objects.requireNonNull(span, "span must not be null");
    final SpanContext ctx = span.getSpanContext();
    final String traceparent =
        "00-" + ctx.getTraceId() + "-" + ctx.getSpanId() + "-" + ctx.getTraceFlags().asHex();
    final WebTransportTraceContext result = WebTransportTraceContext.fromHeaders(traceparent, null);
    return result != null ? result : WebTransportTraceContext.createNew(ctx.isSampled());
  }

  /**
   * Returns the underlying OpenTelemetry tracer.
   *
   * @return tracer instance
   */
  public @NonNull Tracer getTracer() {
    return tracer;
  }
}

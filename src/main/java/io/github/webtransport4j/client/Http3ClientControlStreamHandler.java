package io.github.webtransport4j.client;

import io.github.webtransport4j.server.Http3WtStreamFrameGuard;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http3.Http3ControlStreamFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.http3.Http3UnknownFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inbound HTTP/3 control-stream handler for WebTransport clients per Draft-17 Sections 3.1 and 3.2.
 *
 * <p>Validates the server's {@code SETTINGS_WT_ENABLED} ({@code 0x20b8933b}) value, treating values
 * greater than {@code 1} as a connection error of type {@code H3_SETTINGS_ERROR} ({@code 0x0109}),
 * and overwrites any previously retained connection settings so 0-RTT resumptions always apply the
 * current connection's Section 9.2 values.
 */
public class Http3ClientControlStreamHandler
    extends SimpleChannelInboundHandler<Http3ControlStreamFrame> {

  private static final Logger logger =
      LoggerFactory.getLogger(Http3ClientControlStreamHandler.class);

  @Override
  public void handlerAdded(@NonNull ChannelHandlerContext ctx) throws Exception {
    super.handlerAdded(ctx);
    QuicChannel quic = resolveQuicChannel(ctx);
    if (quic != null) {
      discardRetainedWebTransportSettings(quic);
    }
  }

  /**
   * Clears any WebTransport Section 9.2 settings retained on the {@link QuicChannel} from a prior
   * session or 0-RTT ticket per Draft-17 Section 3.2.
   *
   * @param quic client QUIC connection channel
   */
  public static void discardRetainedWebTransportSettings(@NonNull QuicChannel quic) {
    quic.attr(WebTransportAttributeKeys.WT_ENABLED).set(false);
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(false);
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(null);
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(null);
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(null);
  }

  @Override
  protected void channelRead0(
      @NonNull ChannelHandlerContext ctx, @NonNull Http3ControlStreamFrame frame) {
    if (frame instanceof Http3UnknownFrame) {
      handleUnknownFrame(ctx, (Http3UnknownFrame) frame);
      return;
    }
    if (frame instanceof Http3SettingsFrame) {
      handleSettingsFrame(ctx, (Http3SettingsFrame) frame);
    }
  }

  private void handleUnknownFrame(
      @NonNull ChannelHandlerContext ctx, @NonNull Http3UnknownFrame unknown) {
    if (unknown.type() == WebTransportUtils.BI_STREAM_TYPE) {
      Http3WtStreamFrameGuard.closeWithFrameUnexpected(ctx);
    }
  }

  private void handleSettingsFrame(
      @NonNull ChannelHandlerContext ctx, @NonNull Http3SettingsFrame frame) {
    Http3Settings settings = frame.settings();
    if (settings == null) {
      return;
    }

    QuicChannel quic = resolveQuicChannel(ctx);
    Long wtEnabled = settings.get(WebTransportProtocolSpec.SETTING_WT_ENABLED);
    if (!WebTransportProtocolSpec.isValidServerWtEnabledValue(wtEnabled)) {
      rejectInvalidServerSettings(ctx, quic, wtEnabled);
      return;
    }

    if (quic != null) {
      recordPeerSettings(quic, settings, wtEnabled);
    }
  }

  private void rejectInvalidServerSettings(
      @NonNull ChannelHandlerContext ctx, @Nullable QuicChannel quic, @Nullable Long wtEnabled) {
    logger.warn(
        "Server sent invalid SETTINGS_WT_ENABLED ({}). Closing connection with"
            + " H3_SETTINGS_ERROR (0x0109).",
        wtEnabled);
    if (quic != null) {
      quic.close(true, WebTransportProtocolSpec.H3_SETTINGS_ERROR, Unpooled.EMPTY_BUFFER);
    } else {
      ctx.close();
    }
  }

  private void recordPeerSettings(
      @NonNull QuicChannel quic, @NonNull Http3Settings settings, @Nullable Long wtEnabled) {
    boolean serverEnabled = WebTransportProtocolSpec.isServerWebTransportEnabled(wtEnabled);
    boolean datagramEnabled = Boolean.TRUE.equals(settings.h3DatagramEnabled());
    boolean connectEnabled = Boolean.TRUE.equals(settings.connectProtocolEnabled());

    // Overwrite all Section 9.2 values unconditionally (never retain 0-RTT values).
    quic.attr(WebTransportAttributeKeys.WT_ENABLED).set(serverEnabled);
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID)
        .set(serverEnabled && datagramEnabled && connectEnabled);
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI)
        .set(settings.get(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_UNI));
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI)
        .set(settings.get(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI));
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)
        .set(settings.get(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_DATA));
    quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
  }

  private static QuicChannel resolveQuicChannel(@NonNull ChannelHandlerContext ctx) {
    if (ctx.channel() instanceof QuicStreamChannel) {
      return ((QuicStreamChannel) ctx.channel()).parent();
    }
    if (ctx.channel() instanceof QuicChannel) {
      return (QuicChannel) ctx.channel();
    }
    return null;
  }
}


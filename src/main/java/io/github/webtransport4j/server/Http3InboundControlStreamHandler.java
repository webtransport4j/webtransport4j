package io.github.webtransport4j.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http3.Http3ControlStreamFrame;
import io.netty.handler.codec.http3.Http3GoAwayFrame;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.http3.Http3UnknownFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles inbound HTTP/3 control stream frames per {@code draft-ietf-webtrans-http3-17}.
 *
 * @author https://github.com/sanjomo
 * @date 24/06/26 1:52 pm
 */
public class Http3InboundControlStreamHandler
    extends SimpleChannelInboundHandler<Http3ControlStreamFrame> {

  private static final Logger logger =
      LoggerFactory.getLogger(Http3InboundControlStreamHandler.class);

  @Override
  protected void channelRead0(
      @NonNull ChannelHandlerContext ctx, @NonNull Http3ControlStreamFrame frame) {
    if (frame instanceof Http3UnknownFrame) {
      if (((Http3UnknownFrame) frame).type() == WebTransportUtils.BI_STREAM_TYPE) {
        Http3WtStreamFrameGuard.closeWithFrameUnexpected(ctx);
      }
      return;
    }
    if (frame instanceof Http3GoAwayFrame) {
      QuicChannel connection =
          ctx.channel() instanceof QuicStreamChannel
              ? ((QuicStreamChannel) ctx.channel()).parent()
              : ctx.channel() instanceof QuicChannel ? (QuicChannel) ctx.channel() : null;
      if (connection != null) {
        connection.attr(WebTransportAttributeKeys.CONNECTION_DRAINING).set(true);
        WebTransportSessionManager manager =
            connection.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
        if (manager != null) {
          for (NettyWebTransportSession session : new ObjectArrayList<>(manager.getSessions())) {
            session.markDraining();
          }
        }
      }
      return;
    }
    if (!(frame instanceof Http3SettingsFrame)) {
      return;
    }
    Http3SettingsFrame settingsFrame = (Http3SettingsFrame) frame;
    if (logger.isDebugEnabled()) {
      logger.debug("PEER SETTINGS: {}", settingsFrame);
    }
    Http3Settings settings = settingsFrame.settings();
    if (settings != null) {
      QuicChannel quic = null;
      if (ctx.channel() instanceof QuicStreamChannel) {
        quic = ((QuicStreamChannel) ctx.channel()).parent();
      } else if (ctx.channel() instanceof QuicChannel) {
        quic = (QuicChannel) ctx.channel();
      }

      // Draft-17 Section 3.1: "SETTINGS_WT_ENABLED has no meaning when sent by a client; a server
      // MUST ignore it." Strict Draft-17 servers do not inspect or validate client
      // SETTINGS_WT_ENABLED values.
      boolean valid = Boolean.TRUE.equals(settings.h3DatagramEnabled());
      if (quic != null) {
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(valid);
      }
      // Section 3.1: Verify required setting SETTINGS_H3_DATAGRAM (0x33) is enabled (1).
      // NOTE: Do NOT close the connection immediately here. Per RFC, CONNECT requests can arrive
      // before or after SETTINGS (out of order on different streams). If we close immediately, a
      // late-arriving CONNECT never gets a proper H3_MESSAGE_ERROR reset. Instead, we mark the
      // connection invalid via attributes and let WebTransportHeadersHandler reject CONNECT
      // requests.
      if (!valid) {
        logger.warn(
            "WebTransport requirements not met: Client does not support H3 Datagrams. Treating"
                + " all established sessions as malformed.");
        if (quic != null) {
          WebTransportSessionManager mgr =
              quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
          if (mgr != null) {
            for (NettyWebTransportSession session : new ObjectArrayList<>(mgr.getSessions())) {
              logger.warn(
                  "Resetting established session ID {} with H3_MESSAGE_ERROR",
                  session.getSessionStreamId());
              session.getConnectStream().shutdown(0x010e, session.getConnectStream().newPromise());
            }
          }
        }
        return;
      }
      if (quic != null) {
        Long peerUni = settings.get(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_UNI);
        Long peerBidi = settings.get(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI);
        Long peerData = settings.get(WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_DATA);
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI).set(peerUni);
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI).set(peerBidi);
        quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA).set(peerData);

        boolean flowControlEnabled = WebTransportSessionManager.isFlowControlNegotiated(quic);

        WebTransportSessionManager mgr = quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
        if (mgr != null) {
          for (NettyWebTransportSession session : mgr.getSessions()) {
            if (peerUni != null) {
              session.setPeerSettingsMaxStreamsUni(peerUni);
            }
            if (peerBidi != null) {
              session.setPeerSettingsMaxStreamsBidi(peerBidi);
            }
            if (peerData != null) {
              session.setPeerSettingsMaxData(peerData);
            }
            session.setFlowControlEnabled(flowControlEnabled);
          }
        }
      }
    }
  }
}

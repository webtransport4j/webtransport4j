package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportSession;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http3.Http3SettingsFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author https://github.com/sanjomo
 * @date 24/06/26 1:52 pm
 */
public class Http3InboundControlStreamHandler extends SimpleChannelInboundHandler<Http3SettingsFrame> {

    private static final Logger logger = LoggerFactory.getLogger(Http3InboundControlStreamHandler.class);


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Http3SettingsFrame settingsFrame) {
        if (logger.isDebugEnabled()) {
            logger.debug("PEER SETTINGS: {}", settingsFrame);
        }
        io.netty.handler.codec.http3.Http3Settings settings =
                settingsFrame.settings();
        if (settings != null) {
            QuicChannel quic = null;
            if (ctx.channel() instanceof QuicStreamChannel) {
                quic = ((QuicStreamChannel) ctx.channel()).parent();
            } else if (ctx.channel() instanceof QuicChannel) {
                quic = (QuicChannel) ctx.channel();
            }

            boolean valid = Boolean.TRUE.equals(settings.h3DatagramEnabled());
            if (quic != null) {
                quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(true);
                quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(valid);
            }

            // Section 5.1: Verify required setting SETTINGS_H3_DATAGRAM
            // (0x33) is
            // enabled (1)
            // NOTE: Do NOT close the connection immediately here.
            // Per RFC, CONNECT requests can arrive before or after
            // SETTINGS
            // (out of order on different streams). If we close
            // immediately,
            // a late-arriving CONNECT never gets a proper
            // H3_MESSAGE_ERROR
            // reset. Instead, we mark the connection invalid via
            // attributes
            // and let WebTransportHeadersHandler reject CONNECT
            // requests.
            if (!valid) {
                logger.warn("❌ WebTransport requirements not met: Client does not support H3 Datagrams. Treating all established sessions as malformed.");
                if (quic != null) {
                    WebTransportSessionManager mgr =
                            quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR)
                                    .get();
                    if (mgr != null) {
                        for (WebTransportSession session :
                                new ArrayList<>(mgr.getSessions())) {
                            logger.warn("⚡️ Resetting established session ID {} with H3_MESSAGE_ERROR", session.getSessionStreamId());
                            session
                                    .getConnectStream()
                                    .shutdown(
                                            0x010e,
                                            session.getConnectStream().newPromise());
                        }
                    }
                }
                return;
            }

            if (quic != null) {
                quic.attr(
                                WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI)
                        .set(settings.get(0x2b64L));
                quic.attr(
                                WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI)
                        .set(settings.get(0x2b65L));
                quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)
                        .set(settings.get(0x2b61L));
            }
        }
    }
}

package io.github.webtransport4j.server;

import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http3.DefaultHttp3SettingsFrame;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.jspecify.annotations.NonNull;

/**
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:32 pm
 */
public class QuicChannelInitializer extends ChannelInitializer<QuicChannel> {

    private static final Logger logger = LoggerFactory.getLogger(QuicChannelInitializer.class);

    private final Http3Settings settings;

    private final ExecutorService businessExecutor;

    private final WebTransportServer server;

    private final List<String> allowedOrigins;

    public QuicChannelInitializer(WebTransportServer server, Http3Settings settings, ExecutorService businessExecutor, List<String> allowedOrigins) {
        this.server = server;
        this.settings = settings;
        this.businessExecutor = businessExecutor;
        this.allowedOrigins = allowedOrigins;
    }

    @Override
    protected void initChannel(@NonNull QuicChannel ch) {
        if (logger.isDebugEnabled()) {
            logger.debug("Opening Quic connection : {}", ch.id());
        }
        Long defUni = settings.get(0x2b64L) == null ? Long.valueOf(0L) : settings.get(0x2b64L);
        Long defBidi = settings.get(0x2b65L) == null ? Long.valueOf(0L) : settings.get(0x2b65L);
        Long defData = settings.get(0x2b61L) == null ? Long.valueOf(0L) : settings.get(0x2b61L);
        ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(defUni);
        ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(defBidi);
        ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(defData);
        ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED).set(false);
        ch.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID).set(false);
        if (businessExecutor != null) {
            ch.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).set(businessExecutor);
            logger.info("⚠️  BUSINESS EXECUTOR CONFIGURED: {} (Execution mode: NOT NETTY_EVENT_LOOP)", businessExecutor.getClass().getSimpleName());
        } else {
            logger.info("✓ Business executor is NULL - using direct NETTY_EVENT_LOOP execution");
        }
        long connWriteLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.connection.write.limit", 0L);
        long connReadLimit = WebTransportConfig.getLong("webtransport4j.server.traffic.connection.read.limit", 0L);
        if (connWriteLimit > 0 || connReadLimit > 0) {
            GlobalTrafficShapingHandler connShaper = new GlobalTrafficShapingHandler(ch.eventLoop(), connWriteLimit, connReadLimit);
            ch.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER).set(connShaper);
            ch.closeFuture().addListener(f -> connShaper.release());
        }
        ch.pipeline().addFirst(new QuicGlobalSniffer("GLOBAL-CONN"));
        InetSocketAddress remote = (InetSocketAddress) ch.remoteSocketAddress();
        String ip = Objects.requireNonNull(remote).getAddress().getHostAddress();
        int port = remote.getPort();
        String nettyId = ch.id().asShortText();
        if (logger.isDebugEnabled()) {
            logger.debug("\n🔌 NEW QUIC CONNECTION ESTABLISHED");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("    ├── 🌍 Remote IP:   {}", ip);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("    ├── 🚪 Remote Port: {}", port);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("    └── 🆔 Channel ID:  {}", nettyId);
        }
        ch.attr(WebTransportAttributeKeys.SERVER_KEY).set(this.server);
        ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(new WebTransportSessionManager());
        if (businessExecutor != null) {
            ch.attr(WebTransportAttributeKeys.BUSINESS_EXECUTOR).set(businessExecutor);
            if (logger.isDebugEnabled()) {
                logger.debug("📌 Set BUSINESS_EXECUTOR on channel: {}", businessExecutor.getClass().getSimpleName());
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("📌 BUSINESS_EXECUTOR is NULL on channel (direct event loop execution)");
            }
        }
        ch.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).set(allowedOrigins);
        ch.pipeline().addLast(new WebTransportDatagramDecoder());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added WebTransportDatagramDecoder. Pipeline now: {}", ch.pipeline().names());
        }
        ch.pipeline().addLast(new MessageDispatcher());
        if (logger.isDebugEnabled()) {
            logger.debug("🔧 Added MessageDispatcher. Pipeline now: {}", ch.pipeline().names());
        }
        ch.pipeline().addLast(new Http3ServerConnectionHandler(new WebTransportStreamChannelInitializer(), new Http3InboundControlStreamHandler(), new UnknownStreamHandlerFactory(), new DefaultHttp3SettingsFrame(settings), WebTransportConfig.getBoolean("webtransport4j.http3.qpack.dynamic.table.disabled", true)));
    }
}

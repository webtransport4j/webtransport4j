package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import io.github.webtransport4j.api.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http3.*;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicChannelBootstrap;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import org.jspecify.annotations.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for web transport integration. */
public class WebTransportIntegrationTest {
  private static final Logger log = LoggerFactory.getLogger(WebTransportIntegrationTest.class);

  private NioEventLoopGroup serverGroup;
  private NioEventLoopGroup clientGroup;
  private Channel serverChannel;
  private int port;
  private QuicChannel serverConnectionChannel;
  private WebTransportServer webTransportServer;
  private static final CountDownLatch[] sessionCloseLatch = new CountDownLatch[1];
  private QuicSslContext clientSslContext;

  @Before
  public void setUp() throws Exception {
    System.clearProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute");
    System.clearProperty("webtransport4j.server.ratelimit.max_tracked_ips");
    System.clearProperty("webtransport4j.server.ratelimit.filter_engine");
    System.clearProperty("webtransport4j.server.ratelimit.whitelist");
    System.clearProperty("webtransport4j.server.ratelimit.overrides");
    System.clearProperty("webtransport4j.server.ratelimit.blocklist");
    System.clearProperty("webtransport4j.server.ratelimit.blocklist.bloom_capacity");
    System.clearProperty("webtransport4j.server.ratelimit.blocklist.bloom_fpp");
    System.clearProperty("webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi");
    System.clearProperty("webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni");
    System.clearProperty("webtransport4j.session.resumption.timeout.seconds");
    WebTransportConfig.reload();
    IpRateLimitingHandler.reloadSharedConfig();
    clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols("h3")
            .build();

    setUpServer(10000L);
  }

  private void setUpServer(long initialMaxData) throws Exception {
    System.setProperty("webtransport4j.webtransport.enable_server_push", "false");
    webTransportServer = new WebTransportServer();
    webTransportServer.registerHandler(
        "/test-integration",
        new WebTransportHandler() {
          @Override
          public void onSessionReady(@NonNull WebTransportSession session) {
            log.info("TEST SERVER: Session ready: " + session.getSessionStreamId());
          }

          @Override
          public void onSessionClosed(@NonNull WebTransportSession session) {
            log.info("TEST SERVER: Session closed: " + session.getSessionStreamId());
            if (sessionCloseLatch[0] != null) {
              sessionCloseLatch[0].countDown();
            }
          }

          @Override
          public void onIncomingStream(
              @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
            log.info(
                "TEST SERVER: Incoming stream: "
                    + stream.streamId()
                    + " (bidi="
                    + stream.isBidirectional()
                    + ")");
            stream.onData(
                data -> {
                  String content = new String(data.readBytes(), StandardCharsets.UTF_8);
                  log.info("TEST SERVER: Received on stream " + stream.streamId() + ": " + content);
                  if (stream.isBidirectional()) {
                    stream.writeText(
                        "ACK BI: I received the message from " + session.path() + ": " + content);
                  } else {
                    log.info(
                        "Unidirectional message received from client :"
                            + session.path()
                            + ": "
                            + content);
                  }
                });
          }

          @Override
          public void onDatagramReceived(
              @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
            String content = new String(data.readBytes(), StandardCharsets.UTF_8);
            log.info("TEST SERVER: Received datagram: " + content);
            byte[] respBytes =
                ("ACK DG: I received the message from " + session.path() + ": " + content)
                    .getBytes(StandardCharsets.UTF_8);
            session.sendDatagram(respBytes);
          }
        });
    webTransportServer.registerHandler(
        "/test-reactive",
        new WebTransportHandler() {
          @Override
          public void onIncomingStream(
              @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
            ReactiveWebTransportStream reactiveStream = new ReactiveWebTransportStream(stream);
            Flux<WebTransportBuffer> flux = Flux.from(reactiveStream);
            Flux<WebTransportBuffer> responseFlux = flux.map(buf -> {
              byte[] bytes = buf.readBytes();
              String content = new String(bytes, StandardCharsets.UTF_8);
              return "REACTIVE ACK: " + content;
            })
            .map(ackStr -> (WebTransportBuffer) new DefaultNettyWebTransportBuffer(io.netty.buffer.Unpooled.copiedBuffer(ackStr, StandardCharsets.UTF_8)));
            responseFlux.subscribe(reactiveStream);
          }
        });
    webTransportServer.registerHandler(
        "/test-pure-reactive",
        new ReactiveWebTransportHandlerAdapter(new ReactiveWebTransportHandler() {
          @Override
          public org.reactivestreams.Publisher<Void> onIncomingStream(
              @NonNull ReactiveWebTransportSession session, @NonNull ReactiveWebTransportStream stream) {
            Flux<WebTransportBuffer> responseFlux = Flux.from(stream)
                .map(buf -> {
                  byte[] bytes = buf.readBytes();
                  String content = new String(bytes, StandardCharsets.UTF_8);
                  return "PURE REACTIVE ACK: " + content;
                })
                .map(ackStr -> (WebTransportBuffer) new DefaultNettyWebTransportBuffer(
                    io.netty.buffer.Unpooled.copiedBuffer(ackStr, StandardCharsets.UTF_8)));
            return Mono.fromRunnable(() -> responseFlux.subscribe(stream));
          }
        }));
    serverGroup = new NioEventLoopGroup(1);
    clientGroup = new NioEventLoopGroup(1);

    // Server SSL
    String keyPath = WebTransportConfig.get("webtransport4j.ssl.key.path", null);
    String certPath = WebTransportConfig.get("webtransport4j.ssl.cert.path", null);
    boolean earlyDataEnabled =
        WebTransportConfig.getBoolean("webtransport4j.quic.early.data.enabled", false);

    if (keyPath == null && certPath == null) {
      File keyFile = new File("localhost-key.pem");
      File certFile = new File("localhost.pem");
      if (keyFile.exists() && certFile.exists()) {
        keyPath = keyFile.getAbsolutePath();
        certPath = certFile.getAbsolutePath();
      }
    }

    QuicSslContext serverSslContext;
    if (keyPath != null && certPath != null) {
      serverSslContext =
          QuicSslContextBuilder.forServer(new File(keyPath), null, new File(certPath))
              .earlyData(earlyDataEnabled)
              .sessionCacheSize(20480)
              .sessionTimeout(86400)
              .applicationProtocols(Http3.supportedApplicationProtocols())
              .build();
    } else {
      io.netty.handler.ssl.util.SelfSignedCertificate ssc =
          new io.netty.handler.ssl.util.SelfSignedCertificate();
      serverSslContext =
          QuicSslContextBuilder.forServer(ssc.privateKey(), null, ssc.certificate())
              .earlyData(earlyDataEnabled)
              .sessionCacheSize(20480)
              .sessionTimeout(86400)
              .applicationProtocols(Http3.supportedApplicationProtocols())
              .build();
    }

    if (serverSslContext.sessionContext()
        instanceof io.netty.handler.codec.quic.QuicSslSessionContext) {
      io.netty.handler.codec.quic.SslSessionTicketKey ticketKey =
          new io.netty.handler.codec.quic.SslSessionTicketKey(
              "1234567890123456".getBytes(),
              "1234567890123456".getBytes(),
              "1234567890123456".getBytes());
      ((io.netty.handler.codec.quic.QuicSslSessionContext) serverSslContext.sessionContext())
          .setTicketKeys(new io.netty.handler.codec.quic.SslSessionTicketKey[] {ticketKey});
    }

    // Server Settings
    Http3Settings serverSettings = new Http3Settings((id, value) -> true);
    serverSettings.enableH3Datagram(true);
    serverSettings.enableConnectProtocol(true);
    serverSettings.put(0x2c7cf000L, 1L); // wt_enabled
    serverSettings.put(0x2b64L, 10L); // wt_initial_max_streams_uni
    serverSettings.put(0x2b65L, 10L); // wt_initial_max_streams_bidi
    serverSettings.put(0x2b61L, initialMaxData); // wt_initial_max_data

    // Unidirectional Stream Type Handler Factory on Server
    LongFunction<ChannelHandler> serverUniStreamFactory =
        (streamType) -> {
          if (streamType == 0x54) {
            return new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                WebTransportUtils.addTrafficShapers(ch);
                ch.pipeline()
                    .addLast(
                        new ByteToMessageDecoder() {
                          private boolean sessionHeaderRead = false;

                          @Override
                          protected void decode(
                              ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
                            if (!sessionHeaderRead) {
                              in.markReaderIndex();
                              long sessionId = WebTransportUtils.readVariableLengthInt(in);
                              if (sessionId == -1) {
                                in.resetReaderIndex();
                                return;
                              }
                              ctx.channel()
                                  .attr(WebTransportAttributeKeys.SESSION_ID_KEY)
                                  .set(sessionId);
                              sessionHeaderRead = true;
                            }
                            if (in.isReadable()) {
                              String savedPath =
                                  ctx.channel()
                                      .parent()
                                      .attr(WebTransportAttributeKeys.SESSION_PATH_KEY)
                                      .get();
                              ctx.channel()
                                  .attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)
                                  .set(streamType);
                              ctx.channel()
                                  .attr(WebTransportAttributeKeys.SESSION_PATH_KEY)
                                  .set(savedPath);
                              out.add(in.readRetainedSlice(in.readableBytes()));
                            }
                          }
                        });
                ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
                ch.pipeline().addLast(new WebTransportCapsuleDecoder());
                ch.pipeline().addLast(new WebTransportCapsuleHandler());
                ch.pipeline().addLast(new DefaultMessageDispatcher());
              }
            };
          }
          return new UnknownStreamHandlerFactory().apply(streamType);
        };

    ChannelHandler serverCodec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(serverSslContext)
            .maxIdleTimeout(15000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .tokenHandler(WebTransportServer.getTokenHandler())
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline().addFirst(new IpRateLimitingHandler());
                    serverConnectionChannel = ch;
                    ch.attr(WebTransportAttributeKeys.SERVER_KEY).set(webTransportServer);
                    String originsProp = WebTransportConfig.getNonNull("webtransport4j.webtransport.allowed_origins", "*");
                    java.util.List<String> allowedOrigins = new java.util.ArrayList<>();
                    for (String origin : originsProp.split(",")) {
                      allowedOrigins.add(origin.trim());
                    }
                    ch.attr(WebTransportAttributeKeys.ALLOWED_ORIGINS).set(allowedOrigins);
                    ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR)
                        .set(new WebTransportSessionManager());
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(10L);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(10L);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(initialMaxData);

                    long connWriteLimit =
                        WebTransportConfig.getLong(
                            "webtransport4j.server.traffic.connection.write.limit", 0L);
                    long connReadLimit =
                        WebTransportConfig.getLong(
                            "webtransport4j.server.traffic.connection.read.limit", 0L);
                    if (connWriteLimit > 0 || connReadLimit > 0) {
                      io.netty.handler.traffic.GlobalTrafficShapingHandler connShaper =
                          new io.netty.handler.traffic.GlobalTrafficShapingHandler(
                              ch.eventLoop(), connWriteLimit, connReadLimit);
                      ch.attr(WebTransportAttributeKeys.CONN_TRAFFIC_SHAPER).set(connShaper);
                      ch.closeFuture().addListener(f -> connShaper.release());
                    }

                    ch.pipeline().addLast(new WebTransportDatagramDecoder());
                    ch.pipeline().addLast(new WebTransportCapsuleHandler());
                    ch.pipeline().addLast(new DefaultMessageDispatcher());
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {
                                    log.info(
                                        "SERVER: initChannel for stream ID "
                                            + stream.streamId()
                                            + " | type "
                                            + stream.type());
                                    WebTransportUtils.addTrafficShapers(stream);
                                    stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                    stream.pipeline().addLast(new RawWebTransportHandler());
                                    stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                    stream.pipeline().addLast(new WebTransportHeadersHandler());
                                    stream.pipeline().addLast(new Http3DataToByteBufHandler());
                                    stream.pipeline().addLast(new WebTransportCapsuleDecoder());
                                    stream.pipeline().addLast(new WebTransportCapsuleHandler());
                                    stream.pipeline().addLast(new DefaultMessageDispatcher());
                                  }
                                },
                                new ChannelInboundHandlerAdapter() {
                                  @Override
                                  public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    log.info(
                                        "SERVER: Received message on control stream of class: "
                                            + msg.getClass().getName());
                                    if (msg instanceof Http3SettingsFrame) {
                                      Http3SettingsFrame settingsFrame = (Http3SettingsFrame) msg;
                                      io.netty.handler.codec.http3.Http3Settings settings =
                                          settingsFrame.settings();
                                      log.info("SERVER: Received settings: " + settings);
                                      if (settings != null) {
                                        log.info(
                                            "SERVER: settings wt_enabled="
                                                + settings.get(0x2c7cf000L)
                                                + " | wt_max_data="
                                                + settings.get(0x2b61L));
                                        QuicChannel quic = null;
                                        if (ctx.channel() instanceof QuicStreamChannel) {
                                          quic = ((QuicStreamChannel) ctx.channel()).parent();
                                        } else if (ctx.channel() instanceof QuicChannel) {
                                          quic = (QuicChannel) ctx.channel();
                                        }

                                        boolean valid = Boolean.TRUE.equals(settings.h3DatagramEnabled());
                                        // Set attributes so WebTransportHeadersHandler can check
                                        // them
                                        if (quic != null) {
                                          quic.attr(
                                                  WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED)
                                              .set(true);
                                          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID)
                                              .set(valid);
                                        }

                                        // Section 5.1: Verify required setting SETTINGS_H3_DATAGRAM
                                        // (0x33) is enabled (1)
                                        if (!valid) {
                                          log.info(
                                              "SERVER: WebTransport requirements not met: Client"
                                                  + " does not support H3 Datagrams. Marking"
                                                  + " invalid and resetting sessions.");
                                          // Reset all established sessions with H3_MESSAGE_ERROR
                                          // (0x010e)
                                          if (quic != null) {
                                            WebTransportSessionManager mgr =
                                                quic.attr(WebTransportAttributeKeys.WT_SESSION_MGR)
                                                    .get();
                                            if (mgr != null) {
                                              for (WebTransportSession session :
                                                  new java.util.ArrayList<>(mgr.getSessions())) {
                                                log.info(
                                                    "SERVER: Resetting established session ID "
                                                        + session.getSessionStreamId()
                                                        + " with H3_MESSAGE_ERROR");
                                                session
                                                    .getConnectStream()
                                                    .shutdown(
                                                        0x010e,
                                                        session.getConnectStream().newPromise());
                                              }
                                            }
                                          }
                                          // Don't close connection immediately — let
                                          // WebTransportHeadersHandler
                                          // reject new CONNECT requests via PEER_SETTINGS_VALID
                                          // attribute check
                                          io.netty.util.ReferenceCountUtil.release(msg);
                                          return;
                                        }

                                        log.info("SERVER: Settings parent quic channel: " + quic);
                                        if (quic != null) {
                                          quic.attr(
                                                  WebTransportAttributeKeys
                                                      .PEER_SETTINGS_MAX_STREAMS_UNI)
                                              .set(settings.get(0x2b64L));
                                          quic.attr(
                                                  WebTransportAttributeKeys
                                                      .PEER_SETTINGS_MAX_STREAMS_BIDI)
                                              .set(settings.get(0x2b65L));
                                          quic.attr(
                                                  WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)
                                              .set(settings.get(0x2b61L));
                                        }
                                      }
                                    }
                                    io.netty.util.ReferenceCountUtil.release(msg);
                                  }
                                },
                                serverUniStreamFactory,
                                new DefaultHttp3SettingsFrame(serverSettings),
                                true,
                                (id, value) -> true));
                  }
                })
            .build();

    serverChannel =
        new Bootstrap()
            .group(serverGroup)
            .channel(NioDatagramChannel.class)
            .handler(serverCodec)
            .bind(new InetSocketAddress("127.0.0.1", 0))
            .sync()
            .channel();

    if (WebTransportServer.globalTrafficShaper != null) {
      serverChannel
          .attr(WebTransportAttributeKeys.GLOBAL_TRAFFIC_SHAPER)
          .set(WebTransportServer.globalTrafficShaper);
    }
    port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
  }

  /** Cleans up test fixtures. */
  @After
  public void tearDown() throws Exception {
    if (webTransportServer != null) {
      webTransportServer.registerHandler("/test-integration", null);
    }
    if (serverChannel != null) {
      serverChannel.close().sync();
    }
    if (serverGroup != null) {
      serverGroup.shutdownGracefully();
    }
    if (clientGroup != null) {
      clientGroup.shutdownGracefully();
    }
    if (WebTransportServer.globalTrafficShaper != null) {
      WebTransportServer.globalTrafficShaper.release();
      WebTransportServer.globalTrafficShaper = null;
    }
    System.clearProperty("webtransport4j.server.traffic.global.write.limit");
    System.clearProperty("webtransport4j.server.traffic.global.read.limit");
    System.clearProperty("webtransport4j.server.traffic.connection.write.limit");
    System.clearProperty("webtransport4j.server.traffic.connection.read.limit");
    System.clearProperty("webtransport4j.server.traffic.stream.write.limit");
    System.clearProperty("webtransport4j.server.traffic.stream.read.limit");

    System.clearProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute");
    System.clearProperty("webtransport4j.server.ratelimit.max_tracked_ips");
    System.clearProperty("webtransport4j.server.ratelimit.filter_engine");
    System.clearProperty("webtransport4j.server.ratelimit.whitelist");
    System.clearProperty("webtransport4j.server.ratelimit.overrides");
    System.clearProperty("webtransport4j.server.ratelimit.blocklist");
    System.clearProperty("webtransport4j.server.ratelimit.blocklist.bloom_capacity");
    System.clearProperty("webtransport4j.server.ratelimit.blocklist.bloom_fpp");
    System.clearProperty("webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi");
    System.clearProperty("webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni");
    System.clearProperty("webtransport4j.session.resumption.timeout.seconds");
    WebTransportConfig.reload();
    IpRateLimitingHandler.reloadSharedConfig();
  }

  @Test
  public void testSessionFlowControlLimits() throws Exception {
    // Tear down default server
    tearDown();
    // Start server with initial max data of 20 bytes
    setUpServer(20L);

    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    CountDownLatch closeLatch = new CountDownLatch(1);

    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    // Handshake Connect Stream Creation
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                                // Watch for connect stream closure
                                ctx.channel()
                                    .closeFuture()
                                    .addListener(
                                        future -> {
                                          closeLatch.countDown();
                                        });
                              }
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            log.info(
                                "CLIENT CONNECT stream exceptionCaught: " + cause.getMessage());
                            closeLatch.countDown();
                            ctx.close();
                          }

                          @Override
                          public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                            log.info("CLIENT CONNECT stream channelInactive");
                            closeLatch.countDown();
                            super.channelInactive(ctx);
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("CONNECT handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);
    long sessionId = connectStream[0].streamId();

    // Write 15 bytes to a bidirectional stream (this is within the 20 byte limit)
    CountDownLatch firstWriteLatch = new CountDownLatch(1);
    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                // Hijack pipeline to remove any HTTP/3 request stream codecs
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                ByteBuf data = ch.alloc().directBuffer();
                WebTransportUtils.writeVarInt(data, 0x41);
                WebTransportUtils.writeVarInt(data, sessionId);
                data.writeBytes(
                    "Payload message".getBytes(StandardCharsets.UTF_8)); // 15 bytes payload
                ch.writeAndFlush(data).addListener(wf -> firstWriteLatch.countDown());
              }
            });

    assertTrue("First write failed or timed out", firstWriteLatch.await(5, TimeUnit.SECONDS));

    // Write another 15 bytes to a separate stream (total 30 bytes, exceeding 20 limit)
    CountDownLatch secondWriteLatch = new CountDownLatch(1);
    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                ByteBuf data = ch.alloc().directBuffer();
                WebTransportUtils.writeVarInt(data, 0x41);
                WebTransportUtils.writeVarInt(data, sessionId);
                data.writeBytes(
                    "Second payload!".getBytes(StandardCharsets.UTF_8)); // 15 bytes payload
                ch.writeAndFlush(data).addListener(wf -> secondWriteLatch.countDown());
              }
            });

    assertTrue("Second write failed or timed out", secondWriteLatch.await(5, TimeUnit.SECONDS));

    // Verify that the connectStream is closed by the server due to flow control error (read limit
    // exceeded)
    assertTrue(
        "Connect stream was not closed by flow control error",
        closeLatch.await(5, TimeUnit.SECONDS));
    quicClient.close().sync();
  }

  @Test
  public void testSessionFlowControlImmediateFailure() throws Exception {
    // Tear down default server
    tearDown();
    // Start server with initial max data of 20 bytes
    setUpServer(20L);

    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.attr(WebTransportAttributeKeys.WT_SESSION_MGR)
                        .set(new WebTransportSessionManager());
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(10L);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI).set(10L);
                    ch.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA).set(10000L);

                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {
                                    stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                    stream.pipeline().addLast(new RawWebTransportHandler());
                                    stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                    stream.pipeline().addLast(new WebTransportHeadersHandler());
                                    stream.pipeline().addLast(new Http3DataToByteBufHandler());
                                    stream.pipeline().addLast(new WebTransportCapsuleDecoder());
                                    stream.pipeline().addLast(new WebTransportCapsuleHandler());
                                    stream
                                        .pipeline()
                                        .addLast(
                                            new ChannelInboundHandlerAdapter() {
                                              @Override
                                              public void channelRead(
                                                  ChannelHandlerContext ctx, Object msg)
                                                  throws Exception {
                                                if (msg instanceof Http3SettingsFrame) {
                                                  Http3SettingsFrame settingsFrame =
                                                      (Http3SettingsFrame) msg;
                                                  io.netty.handler.codec.http3.Http3Settings
                                                      settings = settingsFrame.settings();
                                                  if (settings != null) {
                                                    QuicChannel quic =
                                                        ((QuicStreamChannel) ctx.channel())
                                                            .parent();
                                                    if (quic != null) {
                                                      quic.attr(
                                                              WebTransportAttributeKeys
                                                                  .PEER_SETTINGS_MAX_STREAMS_UNI)
                                                          .set(settings.get(0x2b64L));
                                                      quic.attr(
                                                              WebTransportAttributeKeys
                                                                  .PEER_SETTINGS_MAX_STREAMS_BIDI)
                                                          .set(settings.get(0x2b65L));
                                                      quic.attr(
                                                              WebTransportAttributeKeys
                                                                  .PEER_SETTINGS_MAX_DATA)
                                                          .set(settings.get(0x2b61L));
                                                      log.info(
                                                          "CLIENT: Intercepted Settings"
                                                              + " wt_max_data="
                                                              + settings.get(0x2b61L));
                                                    }
                                                  }
                                                }
                                                ctx.fireChannelRead(msg);
                                              }
                                            });
                                  }
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    // Handshake Connect Stream Creation
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();

                                ctx.pipeline().addLast(new Http3DataToByteBufHandler());
                                ctx.pipeline().addLast(new WebTransportCapsuleDecoder());
                                ctx.pipeline().addLast(new WebTransportCapsuleHandler());
                                handshakeLatch.countDown();
                                ctx.pipeline().remove(this);
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("CONNECT handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);

    // Let the client session manager register the CONNECT stream
    WebTransportSessionManager clientMgr =
        quicClient.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    assertNotNull(clientMgr);
    clientMgr.register(connectStream[0]);

    final QuicStreamChannel[] bidiStream = new QuicStreamChannel[1];
    CountDownLatch streamInitLatch = new CountDownLatch(1);

    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });
                ch.pipeline().addLast(new RawWebTransportHandler());
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                bidiStream[0] = f.getNow();
                streamInitLatch.countDown();
              }
            });

    assertTrue(streamInitLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(bidiStream[0]);

    // Write Header [0x41] [SessionID]
    ByteBuf headerData = bidiStream[0].alloc().directBuffer();
    long sessionId = connectStream[0].streamId();
    WebTransportUtils.writeVarInt(headerData, 0x41);
    WebTransportUtils.writeVarInt(headerData, sessionId);

    bidiStream[0].attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionId);
    bidiStream[0].attr(WebTransportAttributeKeys.SERVER_INITIATED_KEY).set(false);

    // Send stream header
    bidiStream[0].writeAndFlush(headerData).sync();

    // Write 15 payload bytes (fits in limit of 20 bytes)
    ByteBuf payload1 = bidiStream[0].alloc().directBuffer();
    payload1.writeBytes("123456789012345".getBytes(StandardCharsets.UTF_8));
    bidiStream[0].writeAndFlush(payload1).sync();

    // Write another 15 payload bytes (total 30, exceeds limit of 20)
    ByteBuf payload2 = bidiStream[0].alloc().directBuffer();
    payload2.writeBytes("abcdefghijklmno".getBytes(StandardCharsets.UTF_8));

    ChannelPromise writePromise = bidiStream[0].newPromise();
    bidiStream[0].writeAndFlush(payload2, writePromise);

    WebTransportSession clientSession = clientMgr.get(sessionId);
    assertNotNull(clientSession);

    // Verify: The write fails immediately because buffering is not supported.
    assertTrue(
        "Immediate failure write did not complete in time",
        writePromise.await(5, TimeUnit.SECONDS));
    assertFalse("Write should have failed due to exceeding flow control", writePromise.isSuccess());
    assertTrue("Cause should be IOException", writePromise.cause() instanceof IOException);

    quicClient.close().sync();
  }

  @Test
  public void testHandshakeDatagramsAndStreams() throws Exception {
    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch datagramLatch = new CountDownLatch(1);

    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    // Unidirectional Stream Type Handler Factory on Client
    LongFunction<ChannelHandler> clientUniStreamFactory =
        (streamType) -> {
          return new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              log.info("CLIENT: initChannel for uni stream ID " + ch.streamId());
              ch.pipeline().addFirst(new WebTransportDetectorHandler());
              ch.pipeline()
                  .addLast(
                      new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                          log.info(
                              "CLIENT: received server-initiated uni stream data: "
                                  + msg.toString(StandardCharsets.UTF_8));
                        }
                      });
            }
          };
        };

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {
                                    log.info(
                                        "CLIENT: initChannel for bidi stream ID "
                                            + stream.streamId());
                                    stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                    stream
                                        .pipeline()
                                        .addLast(
                                            new SimpleChannelInboundHandler<ByteBuf>() {
                                              @Override
                                              protected void channelRead0(
                                                  ChannelHandlerContext ctx, ByteBuf msg) {
                                                log.info(
                                                    "CLIENT: received server-initiated bidi stream"
                                                        + " data: "
                                                        + msg.toString(StandardCharsets.UTF_8));
                                              }
                                            });
                                  }
                                },
                                clientUniStreamFactory,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();
    log.info("DEBUG: Client connected. Active: " + quicClient.isActive());

    // Register WebTransportDatagramDecoder on Client QuicChannel pipeline
    quicClient.pipeline().addLast(new WebTransportDatagramDecoder());
    quicClient
        .pipeline()
        .addLast(
            new SimpleChannelInboundHandler<WebTransportDatagramFrame>() {
              @Override
              protected void channelRead0(
                  ChannelHandlerContext ctx, WebTransportDatagramFrame msg) {
                String content = msg.content().toString(StandardCharsets.UTF_8);
                log.info("DEBUG: Client received Datagram: " + content);
                if (content.contains("ACK DG")) {
                  datagramLatch.countDown();
                }
              }
            });

    // 1. Handshake Connect Stream Creation
    CountDownLatch handshakeLatch = new CountDownLatch(1);
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                log.info("DEBUG: CONNECT Stream pipeline init: " + ch.pipeline().names());
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            log.info(
                                "DEBUG: Client CONNECT stream received message of class: "
                                    + msg.getClass().getName()
                                    + " | Msg: "
                                    + msg);
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              log.info(
                                  "DEBUG: Inbound headers status: "
                                      + headersFrame.headers().status());
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            log.error("DEBUG: CONNECT stream inbound error: " + cause.getMessage());
                            log.error("Exception caught", cause);
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                log.info("DEBUG: CONNECT Stream created successfully. Stream ID: " + ch.streamId());
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers))
                    .addListener(
                        writeFuture -> {
                          log.info(
                              "DEBUG: Write CONNECT headers success: "
                                  + writeFuture.isSuccess()
                                  + " | Cause: "
                                  + writeFuture.cause());
                        });
              } else {
                log.error("DEBUG: CONNECT Stream creation failed! Cause: " + f.cause());
              }
            });

    assertTrue("CONNECT handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);
    long sessionId = connectStream[0].streamId();

    // 2. Datagram Transmission Verification
    ByteBuf dgData = quicClient.alloc().directBuffer();
    WebTransportUtils.writeVarInt(dgData, sessionId);
    dgData.writeBytes("Hello, Datagram integration fire!".getBytes(StandardCharsets.UTF_8));
    quicClient.writeAndFlush(dgData);
    assertTrue("Datagram echo failed or timed out", datagramLatch.await(5, TimeUnit.SECONDS));

    // 3. Bidirectional Stream Transmission Verification
    CountDownLatch bidiEchoLatch = new CountDownLatch(1);
    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                // Hijack pipeline to remove any HTTP/3 request stream codecs automatically added by
                // HTTP/3 client connection handler
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            log.info(
                                "DEBUG: Client bidi stream raw pipeline: "
                                    + ctx.pipeline().names());
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                          log.info("DEBUG: Removed from bidi pipeline: " + name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                      log.info(
                                          "DEBUG: Client bidi stream hijacked pipeline: "
                                              + ctx.pipeline().names());
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            String response = msg.toString(StandardCharsets.UTF_8);
                            log.info("DEBUG: Client bidi stream received: " + response);
                            if (response.contains("ACK BI")) {
                              bidiEchoLatch.countDown();
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            log.error("DEBUG: Client bidi stream error: " + cause.getMessage());
                            log.error("Exception caught", cause);
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                log.info(
                    "DEBUG: Client bidi stream created successfully. Stream ID: " + ch.streamId());
                // Write Header [0x41] [SessionID]
                ByteBuf data = ch.alloc().directBuffer();
                WebTransportUtils.writeVarInt(data, 0x41);
                WebTransportUtils.writeVarInt(data, sessionId);
                // Payload
                data.writeBytes("Payload message".getBytes(StandardCharsets.UTF_8));
                ch.writeAndFlush(data)
                    .addListener(
                        writeFuture -> {
                          log.info(
                              "DEBUG: Write bidi data success: "
                                  + writeFuture.isSuccess()
                                  + " | Cause: "
                                  + writeFuture.cause());
                        });
              } else {
                log.error("DEBUG: Client bidi stream creation failed! Cause: " + f.cause());
              }
            });

    assertTrue(
        "Bidirectional stream echo failed or timed out", bidiEchoLatch.await(5, TimeUnit.SECONDS));
    quicClient.close().sync();
  }

  @Test
  public void testRequirementsNotMetRejection() throws Exception {
    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    // client disables H3 Datagram — violates WebTransport requirements
    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(false);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled

    CountDownLatch resetLatch = new CountDownLatch(1);
    final Throwable[] caughtException = new Throwable[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    // Send a WebTransport CONNECT — server should reject with stream reset
    // because the client's SETTINGS didn't enable H3 Datagrams
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            // ignore
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            caughtException[0] = cause;
                            resetLatch.countDown();
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(
        "CONNECT stream was not reset due to invalid peer settings",
        resetLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(caughtException[0]);
    assertTrue(
        "Expected QuicStreamResetException but got: " + caughtException[0].getClass().getName(),
        caughtException[0] instanceof io.netty.handler.codec.quic.QuicStreamResetException);
    assertEquals(
        0x010eL,
        ((io.netty.handler.codec.quic.QuicStreamResetException) caughtException[0])
            .applicationProtocolCode());

    quicClient.close().sync();
  }

  @Test
  public void testSessionResetStream() throws Exception {
    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    // 1. Handshake Connect Stream Creation
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("CONNECT handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);
    long sessionId = connectStream[0].streamId();

    final QuicStreamChannel[] bidiStream = new QuicStreamChannel[1];
    final CountDownLatch bidiEchoLatch = new CountDownLatch(1);
    final Throwable[] caughtBidiException = new Throwable[1];
    final CountDownLatch bidiResetLatch = new CountDownLatch(1);

    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            String response = msg.toString(StandardCharsets.UTF_8);
                            log.info("DEBUG: Client bidi stream received: " + response);
                            if (response.contains("ACK BI")) {
                              bidiEchoLatch.countDown();
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            log.info("DEBUG: Client bidi stream exception: " + cause);
                            caughtBidiException[0] = cause;
                            bidiResetLatch.countDown();
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                bidiStream[0] = ch;
                ByteBuf data = ch.alloc().directBuffer();
                WebTransportUtils.writeVarInt(data, 0x41);
                WebTransportUtils.writeVarInt(data, sessionId);
                data.writeBytes("Payload message".getBytes(StandardCharsets.UTF_8));
                ch.writeAndFlush(data);
              }
            });

    assertTrue(
        "Bidirectional stream echo failed or timed out", bidiEchoLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(bidiStream[0]);

    // Retrieve server-side session
    WebTransportSessionManager serverMgr =
        serverConnectionChannel.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    assertNotNull(serverMgr);
    WebTransportSession serverSession = serverMgr.get(sessionId);
    assertNotNull(serverSession);

    // Find the server-side stream
    assertEquals(1, serverSession.getActiveClientInitiatedBi().size());
    QuicStreamChannel serverStream = serverSession.getActiveClientInitiatedBi().iterator().next();
    assertNotNull(serverStream);

    // Reset the stream with application error code 500L
    serverSession.resetStream(serverStream, 500L);

    // Verify client bidi stream received the mapped/fallback application error code
    assertTrue("Client did not receive stream reset", bidiResetLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(caughtBidiException[0]);
    assertTrue(
        caughtBidiException[0] instanceof io.netty.handler.codec.quic.QuicStreamResetException);
    io.netty.handler.codec.quic.QuicStreamResetException resetExc =
        (io.netty.handler.codec.quic.QuicStreamResetException) caughtBidiException[0];

    long wtErrorCode =
        WebTransportUtils.httpCodeToWebTransportCode(resetExc.applicationProtocolCode());
    assertEquals(500L, wtErrorCode);

    quicClient.close().sync();
  }

  @Test
  public void testSessionAbort() throws Exception {
    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    final Throwable[] caughtConnectException = new Throwable[1];
    final CountDownLatch connectResetLatch = new CountDownLatch(1);

    // 1. Handshake Connect Stream Creation
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();

                                // Register reset listener on connect stream
                                ctx.pipeline()
                                    .addLast(
                                        new ChannelInboundHandlerAdapter() {
                                          @Override
                                          public void exceptionCaught(
                                              ChannelHandlerContext c, Throwable cause) {
                                            log.info(
                                                "DEBUG: Client CONNECT stream exception: " + cause);
                                            caughtConnectException[0] = cause;
                                            connectResetLatch.countDown();
                                            c.close();
                                          }
                                        });

                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("CONNECT handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);
    long sessionId = connectStream[0].streamId();

    final QuicStreamChannel[] bidiStream = new QuicStreamChannel[1];
    final CountDownLatch bidiEchoLatch = new CountDownLatch(1);
    final Throwable[] caughtBidiException = new Throwable[1];
    final CountDownLatch bidiResetLatch = new CountDownLatch(1);

    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            String response = msg.toString(StandardCharsets.UTF_8);
                            log.info("DEBUG: Client bidi stream received: " + response);
                            if (response.contains("ACK BI")) {
                              bidiEchoLatch.countDown();
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            log.info("DEBUG: Client bidi stream exception: " + cause);
                            caughtBidiException[0] = cause;
                            bidiResetLatch.countDown();
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                bidiStream[0] = ch;
                ByteBuf data = ch.alloc().directBuffer();
                WebTransportUtils.writeVarInt(data, 0x41);
                WebTransportUtils.writeVarInt(data, sessionId);
                data.writeBytes("Payload message".getBytes(StandardCharsets.UTF_8));
                ch.writeAndFlush(data);
              }
            });

    assertTrue(
        "Bidirectional stream echo failed or timed out", bidiEchoLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(bidiStream[0]);

    // Retrieve server-side session
    WebTransportSessionManager serverMgr =
        serverConnectionChannel.attr(WebTransportAttributeKeys.WT_SESSION_MGR).get();
    assertNotNull(serverMgr);
    WebTransportSession serverSession = serverMgr.get(sessionId);
    assertNotNull(serverSession);

    // Abruptly close/abort session with HTTP/3 error code 0x1001L
    serverSession.abort(0x1001L);

    // Verify client connect stream was reset with error code 0x1001L
    assertTrue("Client CONNECT stream did not reset", connectResetLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(caughtConnectException[0]);
    assertTrue(
        caughtConnectException[0] instanceof io.netty.handler.codec.quic.QuicStreamResetException);
    assertEquals(
        0x1001L,
        ((io.netty.handler.codec.quic.QuicStreamResetException) caughtConnectException[0])
            .applicationProtocolCode());

    // Verify client bidi stream was reset with error code 0x1001L
    assertTrue("Client bidi stream did not reset", bidiResetLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(caughtBidiException[0]);
    assertTrue(
        caughtBidiException[0] instanceof io.netty.handler.codec.quic.QuicStreamResetException);
    assertEquals(
        0x1001L,
        ((io.netty.handler.codec.quic.QuicStreamResetException) caughtBidiException[0])
            .applicationProtocolCode());

    quicClient.close().sync();
  }

  private void setUpServerWithThrottling(
      long initialMaxData,
      long globalWrite,
      long globalRead,
      long connWrite,
      long connRead,
      long streamWrite,
      long streamRead)
      throws Exception {
    System.setProperty(
        "webtransport4j.server.traffic.global.write.limit", String.valueOf(globalWrite));
    System.setProperty(
        "webtransport4j.server.traffic.global.read.limit", String.valueOf(globalRead));
    System.setProperty(
        "webtransport4j.server.traffic.connection.write.limit", String.valueOf(connWrite));
    System.setProperty(
        "webtransport4j.server.traffic.connection.read.limit", String.valueOf(connRead));
    System.setProperty(
        "webtransport4j.server.traffic.stream.write.limit", String.valueOf(streamWrite));
    System.setProperty(
        "webtransport4j.server.traffic.stream.read.limit", String.valueOf(streamRead));

    if (globalWrite > 0 || globalRead > 0) {
      serverGroup = new NioEventLoopGroup(1);
      WebTransportServer.globalTrafficShaper =
          new io.netty.handler.traffic.GlobalTrafficShapingHandler(
              serverGroup, globalWrite, globalRead);
    }

    setUpServer(initialMaxData);
  }

  @Test
  public void testTrafficShapingConnectionLimit() throws Exception {
    tearDown();
    setUpServerWithThrottling(10000L, 0L, 0L, 500L, 0L, 0L, 0L);

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);

    long sessionId = connectStream[0].streamId();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append("A");
    }
    String chunkString = sb.toString();

    CountDownLatch echoLatch = new CountDownLatch(1);
    final int[] receivedBytes = new int[1];
    long startTime = System.currentTimeMillis();
    final long[] endTime = new long[1];

    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            receivedBytes[0] += msg.readableBytes();
                            if (receivedBytes[0] >= 2000) {
                              endTime[0] = System.currentTimeMillis();
                              echoLatch.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                for (int i = 0; i < 10; i++) {
                  final int idx = i;
                  ch.eventLoop()
                      .schedule(
                          () -> {
                            ByteBuf data = ch.alloc().directBuffer();
                            WebTransportUtils.writeVarInt(data, 0x41);
                            WebTransportUtils.writeVarInt(data, sessionId);
                            data.writeBytes(chunkString.getBytes(StandardCharsets.UTF_8));
                            ch.writeAndFlush(data);
                          },
                          idx * 50,
                          TimeUnit.MILLISECONDS);
                }
              }
            });

    assertTrue("Transfer timed out", echoLatch.await(30, TimeUnit.SECONDS));
    long duration = endTime[0] - startTime;
    log.info("Connection Throttling Test Duration: " + duration + " ms");
    assertTrue("Throttling did not delay transfer: " + duration + "ms", duration >= 2000);

    quicClient.close().sync();
  }

  @Test
  public void testTrafficShapingStreamLimit() throws Exception {
    tearDown();
    setUpServerWithThrottling(10000L, 0L, 0L, 0L, 0L, 500L, 0L);

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);

    long sessionId = connectStream[0].streamId();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append("A");
    }
    String chunkString = sb.toString();

    CountDownLatch echoLatch = new CountDownLatch(1);
    final int[] receivedBytes = new int[1];
    long startTime = System.currentTimeMillis();
    final long[] endTime = new long[1];

    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            receivedBytes[0] += msg.readableBytes();
                            if (receivedBytes[0] >= 2000) {
                              endTime[0] = System.currentTimeMillis();
                              echoLatch.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                for (int i = 0; i < 10; i++) {
                  final int idx = i;
                  ch.eventLoop()
                      .schedule(
                          () -> {
                            ByteBuf data = ch.alloc().directBuffer();
                            WebTransportUtils.writeVarInt(data, 0x41);
                            WebTransportUtils.writeVarInt(data, sessionId);
                            data.writeBytes(chunkString.getBytes(StandardCharsets.UTF_8));
                            ch.writeAndFlush(data);
                          },
                          idx * 50,
                          TimeUnit.MILLISECONDS);
                }
              }
            });

    assertTrue("Transfer timed out", echoLatch.await(30, TimeUnit.SECONDS));
    long duration = endTime[0] - startTime;
    log.info("Stream Throttling Test Duration: " + duration + " ms");
    assertTrue("Throttling did not delay transfer: " + duration + "ms", duration >= 2000);

    quicClient.close().sync();
  }

  @Test
  public void testTrafficShapingGlobalLimit() throws Exception {
    tearDown();
    setUpServerWithThrottling(10000L, 1000L, 0L, 0L, 0L, 0L, 0L);

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L);
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    // Connection 1
    CountDownLatch handshakeLatch1 = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream1 = new QuicStreamChannel[1];
    Channel clientChannel1 =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(
                Http3.newQuicClientCodecBuilder()
                    .sslContext(clientSslContext)
                    .maxIdleTimeout(15000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100)
                    .initialMaxStreamsUnidirectional(100)
                    .datagram(10000, 10000)
                    .build())
            .bind(0)
            .sync()
            .channel();
    QuicChannel quicClient1 =
        QuicChannel.newBootstrap(clientChannel1)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port))
            .connect()
            .sync()
            .getNow();

    Http3.newRequestStream(
            quicClient1,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream1[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch1.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch1.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream1[0]);

    // Connection 2
    CountDownLatch handshakeLatch2 = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream2 = new QuicStreamChannel[1];
    Channel clientChannel2 =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(
                Http3.newQuicClientCodecBuilder()
                    .sslContext(clientSslContext)
                    .maxIdleTimeout(15000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100)
                    .initialMaxStreamsUnidirectional(100)
                    .datagram(10000, 10000)
                    .build())
            .bind(0)
            .sync()
            .channel();
    QuicChannel quicClient2 =
        QuicChannel.newBootstrap(clientChannel2)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port))
            .connect()
            .sync()
            .getNow();

    Http3.newRequestStream(
            quicClient2,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream2[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch2.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch2.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream2[0]);
    long sessionId2 = connectStream2[0].streamId();

    // Data preparation
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append("A");
    }
    String chunkString = sb.toString();

    CountDownLatch echoLatch = new CountDownLatch(2);
    final int[] receivedBytes1 = new int[1];
    final int[] receivedBytes2 = new int[1];
    final long startTime = System.currentTimeMillis();
    final long[] endTime = new long[1];

    // Start Stream 1
    long sessionId1 = connectStream1[0].streamId();
    quicClient1
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            receivedBytes1[0] += msg.readableBytes();
                            if (receivedBytes1[0] >= 2000) {
                              echoLatch.countDown();
                              if (echoLatch.getCount() == 0) {
                                endTime[0] = System.currentTimeMillis();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                for (int i = 0; i < 10; i++) {
                  final int idx = i;
                  ch.eventLoop()
                      .schedule(
                          () -> {
                            ByteBuf data = ch.alloc().directBuffer();
                            WebTransportUtils.writeVarInt(data, 0x41);
                            WebTransportUtils.writeVarInt(data, sessionId1);
                            data.writeBytes(chunkString.getBytes(StandardCharsets.UTF_8));
                            ch.writeAndFlush(data);
                          },
                          idx * 50,
                          TimeUnit.MILLISECONDS);
                }
              }
            });

    // Start Stream 2
    quicClient2
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            receivedBytes2[0] += msg.readableBytes();
                            if (receivedBytes2[0] >= 2000) {
                              echoLatch.countDown();
                              if (echoLatch.getCount() == 0) {
                                endTime[0] = System.currentTimeMillis();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                for (int i = 0; i < 10; i++) {
                  final int idx = i;
                  ch.eventLoop()
                      .schedule(
                          () -> {
                            ByteBuf data = ch.alloc().directBuffer();
                            WebTransportUtils.writeVarInt(data, 0x41);
                            WebTransportUtils.writeVarInt(data, sessionId2);
                            data.writeBytes(chunkString.getBytes(StandardCharsets.UTF_8));
                            ch.writeAndFlush(data);
                          },
                          idx * 50,
                          TimeUnit.MILLISECONDS);
                }
              }
            });

    assertTrue("Transfer timed out", echoLatch.await(15, TimeUnit.SECONDS));
    long duration = System.currentTimeMillis() - startTime;
    log.info("Global Throttling Test Duration: " + duration + " ms");
    assertTrue("Throttling did not delay transfer: " + duration + "ms", duration >= 1500);

    quicClient1.close().sync();
    quicClient2.close().sync();
  }

  @Test
  public void testTrafficShapingReadLimit() throws Exception {
    tearDown();
    setUpServerWithThrottling(10000L, 0L, 0L, 0L, 0L, 0L, 500L);

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(15000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);

    long sessionId = connectStream[0].streamId();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append("A");
    }
    String chunkString = sb.toString();

    CountDownLatch echoLatch = new CountDownLatch(1);
    final int[] receivedBytes = new int[1];
    long startTime = System.currentTimeMillis();
    final long[] endTime = new long[1];

    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            receivedBytes[0] += msg.readableBytes();
                            if (receivedBytes[0] >= 2000) {
                              endTime[0] = System.currentTimeMillis();
                              echoLatch.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                for (int i = 0; i < 10; i++) {
                  final int idx = i;
                  ch.eventLoop()
                      .schedule(
                          () -> {
                            ByteBuf data = ch.alloc().directBuffer();
                            WebTransportUtils.writeVarInt(data, 0x41);
                            WebTransportUtils.writeVarInt(data, sessionId);
                            data.writeBytes(chunkString.getBytes(StandardCharsets.UTF_8));
                            ch.writeAndFlush(data);
                          },
                          idx * 100,
                          TimeUnit.MILLISECONDS);
                }
              }
            });

    assertTrue("Transfer timed out", echoLatch.await(15, TimeUnit.SECONDS));
    long duration = endTime[0] - startTime;
    log.info("Stream Read Throttling Test Duration: " + duration + " ms");
    assertTrue("Throttling did not delay transfer: " + duration + "ms", duration >= 1500);

    quicClient.close().sync();
  }

  @Test
  public void testTrafficShapingMultipleLimitsCoexistence() throws Exception {
    tearDown();
    setUpServerWithThrottling(10000L, 10000L, 0L, 5000L, 0L, 500L, 0L);

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(15000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);

    long sessionId = connectStream[0].streamId();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append("A");
    }
    String chunkString = sb.toString();

    CountDownLatch echoLatch = new CountDownLatch(1);
    final int[] receivedBytes = new int[1];
    long startTime = System.currentTimeMillis();
    final long[] endTime = new long[1];

    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<ByteBuf>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            receivedBytes[0] += msg.readableBytes();
                            if (receivedBytes[0] >= 2000) {
                              endTime[0] = System.currentTimeMillis();
                              echoLatch.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                for (int i = 0; i < 10; i++) {
                  final int idx = i;
                  ch.eventLoop()
                      .schedule(
                          () -> {
                            ByteBuf data = ch.alloc().directBuffer();
                            WebTransportUtils.writeVarInt(data, 0x41);
                            WebTransportUtils.writeVarInt(data, sessionId);
                            data.writeBytes(chunkString.getBytes(StandardCharsets.UTF_8));
                            ch.writeAndFlush(data);
                          },
                          idx * 50,
                          TimeUnit.MILLISECONDS);
                }
              }
            });

    assertTrue("Transfer timed out", echoLatch.await(15, TimeUnit.SECONDS));
    long duration = endTime[0] - startTime;
    log.info("Coexistence Throttling Test Duration: " + duration + " ms");
    assertTrue("Throttling did not delay transfer: " + duration + "ms", duration >= 1500);

    quicClient.close().sync();
  }

  @Test
  public void testMalformedSettingsStreamReset() throws Exception {
    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(false); // REQUIRED SETTING IS FALSE/MISSING
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch resetLatch = new CountDownLatch(1);
    final Throwable[] caughtException = new Throwable[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    // Connect request
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            // ignore
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            caughtException[0] = cause;
                            resetLatch.countDown();
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(
        "CONNECT stream did not reset due to malformed/invalid settings",
        resetLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(caughtException[0]);
    assertTrue(
        "Expected QuicStreamResetException",
        caughtException[0] instanceof io.netty.handler.codec.quic.QuicStreamResetException);
    // H3_MESSAGE_ERROR is 0x010e (270)
    assertEquals(
        0x010eL,
        ((io.netty.handler.codec.quic.QuicStreamResetException) caughtException[0])
            .applicationProtocolCode());

    quicClient.close().sync();
  }

  @Test
  public void testFragmentedHeaderParsingIntegration() throws Exception {
    // Build client ssl context
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    CountDownLatch handshakeLatch = new CountDownLatch(1);

    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {
                                    stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                    stream
                                        .pipeline()
                                        .addLast(
                                            new SimpleChannelInboundHandler<ByteBuf>() {
                                              @Override
                                              protected void channelRead0(
                                                  ChannelHandlerContext ctx, ByteBuf msg) {
                                                // ignore
                                              }
                                            });
                                  }
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    // 1. Handshake Connect Stream Creation
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("CONNECT handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);
    long sessionId = connectStream[0].streamId();

    // 2. Bidirectional Stream with Fragmented Headers
    CountDownLatch bidiEchoLatch = new CountDownLatch(1);
    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                            ctx.channel()
                                .eventLoop()
                                .execute(
                                    () -> {
                                      java.util.List<String> toRemove = new java.util.ArrayList<>();
                                      for (String name : ctx.pipeline().names()) {
                                        ChannelHandler h = ctx.pipeline().get(name);
                                        if (h != null
                                            && h != this
                                            && (name.contains("Http3")
                                                || h.getClass().getName().contains("Http3"))) {
                                          toRemove.add(name);
                                        }
                                      }
                                      for (String name : toRemove) {
                                        try {
                                          ctx.pipeline().remove(name);
                                        } catch (Exception expected) {
                                        }
                                      }
                                    });
                            super.handlerAdded(ctx);
                          }
                        });

                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            ByteBuf buf = null;
                            if (msg instanceof ByteBuf) {
                              buf = (ByteBuf) msg;
                            } else if (msg instanceof io.netty.handler.codec.http3.Http3DataFrame) {
                              buf = ((io.netty.handler.codec.http3.Http3DataFrame) msg).content();
                            } else if (msg instanceof WebTransportStreamFrame) {
                              buf = ((WebTransportStreamFrame) msg).content();
                            }
                            if (buf == null) {
                              return;
                            }
                            String response = buf.toString(StandardCharsets.UTF_8);
                            log.info("DEBUG: Fragmented test received: " + response);
                            if (response.contains("ACK BI")
                                && response.contains("Fragmented headers work!")) {
                              bidiEchoLatch.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                // We send the stream type first as a single byte
                ByteBuf piece1 = ch.alloc().directBuffer(1);
                WebTransportUtils.writeVarInt(piece1, 0x41);
                ch.writeAndFlush(piece1)
                    .addListener(
                        f1 -> {
                          // After piece 1 is sent, send piece 2 (Session ID)
                          ch.eventLoop()
                              .schedule(
                                  () -> {
                                    ByteBuf piece2 = ch.alloc().directBuffer();
                                    WebTransportUtils.writeVarInt(piece2, sessionId);
                                    ch.writeAndFlush(piece2)
                                        .addListener(
                                            f2 -> {
                                              // After piece 2 is sent, send piece 3 (Payload)
                                              ch.eventLoop()
                                                  .schedule(
                                                      () -> {
                                                        ByteBuf piece3 = ch.alloc().directBuffer();
                                                        piece3.writeBytes(
                                                            "Fragmented headers work!"
                                                                .getBytes(StandardCharsets.UTF_8));
                                                        ch.writeAndFlush(piece3);
                                                      },
                                                      50,
                                                      TimeUnit.MILLISECONDS);
                                            });
                                  },
                                  50,
                                  TimeUnit.MILLISECONDS);
                        });
              }
            });

    assertTrue(
        "Bidirectional stream exchange with fragmented headers failed or timed out",
        bidiEchoLatch.await(5, TimeUnit.SECONDS));

    quicClient.close().sync();
  }

  @Test
  public void testChunkedInputSupport() throws Exception {
    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    CountDownLatch chunkedEchoLatch = new CountDownLatch(1);

    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();
    QuicChannel quicClient =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress(io.netty.util.NetUtil.LOCALHOST4, port))
            .connect()
            .sync()
            .getNow();

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers
                    .method("CONNECT")
                    .scheme("https")
                    .path("/test-integration")
                    .authority("localhost")
                    .set(":protocol", "webtransport");
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("CONNECT handshake failed", handshakeLatch.await(5, TimeUnit.SECONDS));
    long sessionId = connectStream[0].streamId();

    StringBuilder accumulatedEcho = new StringBuilder();
    String expectedPayload = "CHUNK_1_DATA|CHUNK_2_DATA|CHUNK_3_DATA";

    quicClient.createStream(
        QuicStreamType.BIDIRECTIONAL,
        new ChannelInitializer<QuicStreamChannel>() {
          @Override
          protected void initChannel(QuicStreamChannel ch) {
            ch.pipeline()
                .addFirst(
                    new ChannelInboundHandlerAdapter() {
                      @Override
                      public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                        ctx.channel()
                            .eventLoop()
                            .execute(
                                () -> {
                                  java.util.List<String> toRemove = new java.util.ArrayList<>();
                                  for (String name : ctx.pipeline().names()) {
                                    if (name.startsWith("Http3RequestStream")) {
                                      toRemove.add(name);
                                    }
                                  }
                                  toRemove.forEach(name -> ctx.pipeline().remove(name));

                                  ctx.pipeline().addLast(new WebTransportChunkedWriteHandler());
                                  ctx.pipeline()
                                      .addLast(
                                          new SimpleChannelInboundHandler<ByteBuf>() {
                                            @Override
                                            protected void channelRead0(
                                                ChannelHandlerContext ctx, ByteBuf msg) {
                                              accumulatedEcho.append(
                                                  msg.toString(StandardCharsets.UTF_8));
                                              if (accumulatedEcho
                                                  .toString()
                                                  .contains(expectedPayload)) {
                                                chunkedEchoLatch.countDown();
                                              }
                                            }
                                          });

                                  ByteBuf header = ctx.alloc().directBuffer();
                                  WebTransportUtils.writeVarInt(
                                      header, WebTransportUtils.BI_STREAM_TYPE);
                                  WebTransportUtils.writeVarInt(header, sessionId);
                                  ctx.channel()
                                      .writeAndFlush(header)
                                      .addListener(
                                          f -> {
                                            BinarySource source =
                                                new BinarySource() {
                                                  byte[] data =
                                                      expectedPayload.getBytes(
                                                          StandardCharsets.UTF_8);
                                                  int progress = 0;

                                                  @Override
                                                  public int read(
                                                      java.nio.@NonNull ByteBuffer buffer) {
                                                    if (progress >= data.length) {
                                                      return -1;
                                                    }
                                                    int toRead =
                                                        Math.min(
                                                            data.length - progress,
                                                            buffer.remaining());
                                                    toRead = Math.min(toRead, 5);
                                                    buffer.put(data, progress, toRead);
                                                    progress += toRead;
                                                    return toRead;
                                                  }

                                                  @Override
                                                  public long size() {
                                                    return data.length;
                                                  }

                                                  @Override
                                                  public boolean hasKnownSize() {
                                                    return true;
                                                  }
                                                };
                                            ctx.channel()
                                                .writeAndFlush(
                                                    new BinarySourceChunkedInput(source));
                                          });
                                });
                      }
                    });
          }
        });

    assertTrue("Chunked echo timed out", chunkedEchoLatch.await(5, TimeUnit.SECONDS));
    quicClient.close().sync();
  }

  @Test
  public void testSessionResumptionIntegration() throws Exception {
    // 1. Setup client ssl context and settings
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient1 = bootstrap.connect().sync().getNow();

    CountDownLatch handshake1Latch = new CountDownLatch(1);
    final String[] tokenContainer = new String[1];

    Http3.newRequestStream(
            quicClient1,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                CharSequence rawToken = headersFrame.headers().get("sec-webtransport-resumption-token");
                                if (rawToken != null) {
                                  tokenContainer[0] = rawToken.toString();
                                }
                                handshake1Latch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("First handshake failed or timed out", handshake1Latch.await(5, TimeUnit.SECONDS));
    assertNotNull("Resumption token must be provided by server", tokenContainer[0]);

    // Close first connection to register the session as orphaned
    quicClient1.close().sync();

    // Sleep to let server register the session in resumption cache
    Thread.sleep(200);

    // Now establish a second connection to resume it
    QuicChannel quicClient2 = bootstrap.connect().sync().getNow();
    CountDownLatch handshake2Latch = new CountDownLatch(1);
    final String[] returnedTokenContainer = new String[1];
    final QuicStreamChannel[] resumedConnectStream = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient2,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                CharSequence rawToken = headersFrame.headers().get("sec-webtransport-resumption-token");
                                if (rawToken != null) {
                                  returnedTokenContainer[0] = rawToken.toString();
                                }
                                resumedConnectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshake2Latch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                // Pass the resumption token!
                headers.set("webtransport-resumption-token", tokenContainer[0]);
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Resumed handshake failed or timed out", handshake2Latch.await(5, TimeUnit.SECONDS));
    org.junit.Assert.assertNotEquals(tokenContainer[0], returnedTokenContainer[0]);
    assertNotNull(resumedConnectStream[0]);

    // Send payload data over the resumed session to verify connection binding is active and functional
    CountDownLatch dataLatch = new CountDownLatch(1);
    quicClient2
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) {
                            ctx.channel().eventLoop().execute(() -> {
                              java.util.List<String> toRemove = new java.util.ArrayList<>();
                              for (String name : ctx.pipeline().names()) {
                                ChannelHandler h = ctx.pipeline().get(name);
                                if (h != null && h != this) {
                                  toRemove.add(name);
                                }
                              }
                              for (String name : toRemove) {
                                ctx.pipeline().remove(name);
                              }
                              ctx.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext c, Object msg) {
                                  if (msg instanceof io.netty.buffer.ByteBuf) {
                                    io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
                                    String text = buf.toString(StandardCharsets.UTF_8);
                                    if (text.startsWith("ACK BI:")) {
                                      dataLatch.countDown();
                                    }
                                  }
                                }
                              });
                            });
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                io.netty.buffer.ByteBuf header = ch.alloc().buffer();
                WebTransportUtils.writeVarInt(header, 0x41);
                WebTransportUtils.writeVarInt(header, resumedConnectStream[0].streamId());
                ch.write(header);

                // Write stream data payload
                ch.writeAndFlush(io.netty.buffer.Unpooled.copiedBuffer("Payload message", StandardCharsets.UTF_8));
              }
            });

    assertTrue("Resumed stream data transmission failed or timed out", dataLatch.await(5, TimeUnit.SECONDS));

    quicClient2.close().sync();
  }

  @Test
  public void testSessionReactiveStreamIntegration() throws Exception {
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-reactive");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);

    CountDownLatch dataLatch = new CountDownLatch(1);
    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) {
                            ctx.channel().eventLoop().execute(() -> {
                              java.util.List<String> toRemove = new java.util.ArrayList<>();
                              for (String name : ctx.pipeline().names()) {
                                ChannelHandler h = ctx.pipeline().get(name);
                                if (h != null && h != this) {
                                  toRemove.add(name);
                                }
                              }
                              for (String name : toRemove) {
                                ctx.pipeline().remove(name);
                              }
                              ctx.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext c, Object msg) {
                                  if (msg instanceof io.netty.buffer.ByteBuf) {
                                    io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
                                    String text = buf.toString(StandardCharsets.UTF_8);
                                    if (text.startsWith("REACTIVE ACK: Hello Reactor!")) {
                                      dataLatch.countDown();
                                    }
                                  }
                                }
                              });
                            });
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                io.netty.buffer.ByteBuf header = ch.alloc().buffer();
                WebTransportUtils.writeVarInt(header, 0x41);
                WebTransportUtils.writeVarInt(header, connectStream[0].streamId());
                ch.write(header);

                // Write stream data payload
                ch.writeAndFlush(io.netty.buffer.Unpooled.copiedBuffer("Hello Reactor!", StandardCharsets.UTF_8));
              }
            });

    assertTrue("Reactive stream ACK failed or timed out", dataLatch.await(5, TimeUnit.SECONDS));

    quicClient.close().sync();
  }

  @Test
  public void testPureReactiveHandlerIntegration() throws Exception {
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-pure-reactive");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);

    CountDownLatch dataLatch = new CountDownLatch(1);
    quicClient
        .createStream(
            QuicStreamType.BIDIRECTIONAL,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addFirst(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void handlerAdded(ChannelHandlerContext ctx) {
                            ctx.channel().eventLoop().execute(() -> {
                              java.util.List<String> toRemove = new java.util.ArrayList<>();
                              for (String name : ctx.pipeline().names()) {
                                ChannelHandler h = ctx.pipeline().get(name);
                                if (h != null && h != this) {
                                  toRemove.add(name);
                                }
                              }
                              for (String name : toRemove) {
                                ctx.pipeline().remove(name);
                              }
                              ctx.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext c, Object msg) {
                                  if (msg instanceof io.netty.buffer.ByteBuf) {
                                    io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
                                    String text = buf.toString(StandardCharsets.UTF_8);
                                    if (text.startsWith("PURE REACTIVE ACK: Hello Pure Reactor!")) {
                                      dataLatch.countDown();
                                    }
                                  }
                                }
                              });
                            });
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                io.netty.buffer.ByteBuf header = ch.alloc().buffer();
                WebTransportUtils.writeVarInt(header, 0x41);
                WebTransportUtils.writeVarInt(header, connectStream[0].streamId());
                ch.write(header);

                // Write stream data payload
                ch.writeAndFlush(io.netty.buffer.Unpooled.copiedBuffer("Hello Pure Reactor!", StandardCharsets.UTF_8));
              }
            });

    assertTrue("Pure Reactive stream ACK failed or timed out", dataLatch.await(5, TimeUnit.SECONDS));

    quicClient.close().sync();
  }

  @Test
  public void testDynamicRateLimitReloadIntegration() throws Exception {
    // 1. Establish connection to verify it normally succeeds
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamsBidirectional(100)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      // Set limit=1 and clear whitelist
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute=1",
          "webtransport4j.server.ratelimit.whitelist="
      ));
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();

      // 1. Connect first time: should succeed
      QuicChannel quicClient = bootstrap.connect().sync().getNow();
      assertTrue(quicClient.isActive());
      quicClient.close().sync();

      // 3. Attempt to connect: should be rejected/closed immediately by the server
      Channel clientChannel2 =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(
                  Http3.newQuicClientCodecBuilder()
                      .sslContext(clientSslContext)
                      .maxIdleTimeout(2000, TimeUnit.MILLISECONDS)
                      .build())
              .bind(0)
              .sync()
              .channel();

      QuicChannelBootstrap bootstrap2 = QuicChannel.newBootstrap(clientChannel2)
          .handler(new ChannelInitializer<QuicChannel>() {
            @Override protected void initChannel(QuicChannel ch) {}
          })
          .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      boolean connectFailed = false;
      try {
        QuicChannel quicClient2 = bootstrap2.connect().sync().getNow();
        quicClient2.closeFuture().await(2000, TimeUnit.MILLISECONDS);
        if (!quicClient2.isActive()) {
          connectFailed = true;
        }
        quicClient2.close();
      } catch (Exception e) {
        connectFailed = true;
      }
      assertTrue("Connection from blocked IP should fail", connectFailed);

    } finally {
      // 4. Clean up the dynamic override file
      if (tempFile.exists()) {
        tempFile.delete();
      }
      // Reload again to restore default rules
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();
    }
  }

  @Test
  public void testDynamicWhitelistReloadIntegration() throws Exception {
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled

    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      // 1. Set max connections to 0 (blocks all) but add localhost to whitelist
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute=0",
          "webtransport4j.server.ratelimit.whitelist=127.0.0.1,::1,0:0:0:0:0:0:0:1"
      ));

      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();

      // 2. Connect client: should succeed because it bypasses via whitelist
      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(
                  Http3.newQuicClientCodecBuilder()
                      .sslContext(clientSslContext)
                      .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                      .build())
              .bind(0)
              .sync()
              .channel();

      QuicChannelBootstrap bootstrap = QuicChannel.newBootstrap(clientChannel)
          .handler(new ChannelInitializer<QuicChannel>() {
            @Override protected void initChannel(QuicChannel ch) {}
          })
          .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      QuicChannel quicClient = bootstrap.connect().sync().getNow();
      assertTrue("Whitelisted connection should succeed despite 0 rate limit", quicClient.isActive());
      quicClient.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();
    }
  }

  @Test
  public void testDynamicOverridesReloadIntegration() throws Exception {
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled

    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      // 1. Set max connections to 0 (blocks all) but add override limit of 5 for localhost
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute=0",
          "webtransport4j.server.ratelimit.whitelist=", // clear whitelist
          "webtransport4j.server.ratelimit.overrides=127.0.0.1:5,::1:5,0:0:0:0:0:0:0:1:5"
      ));

      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();

      // 2. Connect client: should succeed because it bypasses via overrides
      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(
                  Http3.newQuicClientCodecBuilder()
                      .sslContext(clientSslContext)
                      .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                      .build())
              .bind(0)
              .sync()
              .channel();

      QuicChannelBootstrap bootstrap = QuicChannel.newBootstrap(clientChannel)
          .handler(new ChannelInitializer<QuicChannel>() {
            @Override protected void initChannel(QuicChannel ch) {}
          })
          .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      QuicChannel quicClient = bootstrap.connect().sync().getNow();
      assertTrue("Overrides connection should succeed despite 0 rate limit", quicClient.isActive());
      quicClient.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();
    }
  }

  @Test
  public void testDynamicBlocklistReloadIntegration() throws Exception {
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled

    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      // 1. Set max connections to 100 (allows all) but add localhost to blocklist
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.server.ratelimit.max_connections_per_ip_per_minute=100",
          "webtransport4j.server.ratelimit.whitelist=", // clear whitelist
          "webtransport4j.server.ratelimit.blocklist=127.0.0.1,::1,0:0:0:0:0:0:0:1",
          "webtransport4j.server.ratelimit.blocklist.bloom_capacity=1000",
          "webtransport4j.server.ratelimit.blocklist.bloom_fpp=0.01"
      ));

      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();

      // 2. Connect client: should be rejected
      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(
                  Http3.newQuicClientCodecBuilder()
                      .sslContext(clientSslContext)
                      .maxIdleTimeout(2000, TimeUnit.MILLISECONDS)
                      .build())
              .bind(0)
              .sync()
              .channel();

      QuicChannelBootstrap bootstrap = QuicChannel.newBootstrap(clientChannel)
          .handler(new ChannelInitializer<QuicChannel>() {
            @Override protected void initChannel(QuicChannel ch) {}
          })
          .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      boolean connectFailed = false;
      try {
        QuicChannel quicClient = bootstrap.connect().sync().getNow();
        quicClient.closeFuture().await(2000, TimeUnit.MILLISECONDS);
        if (!quicClient.isActive()) {
          connectFailed = true;
        }
        quicClient.close();
      } catch (Exception e) {
        connectFailed = true;
      }
      assertTrue("Connection from blocked IP should fail", connectFailed);

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();
    }
  }

  @Test
  public void testDynamicFlowControlStreamsLimitIntegration() throws Exception {
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      // Set absolute bidi streams limit to 2
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.webtransport.initial.max.streams.bidi=2",
          "webtransport4j.webtransport.flowcontrol.max_absolute_streams.bidi=2",
          "webtransport4j.webtransport.initial.max.streams.uni=2",
          "webtransport4j.webtransport.flowcontrol.max_absolute_streams.uni=2"
      ));
      WebTransportConfig.reload();

      sessionCloseLatch[0] = new CountDownLatch(1);
      QuicChannel quicClient = bootstrap.connect().sync().getNow();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

      Http3.newRequestStream(
              quicClient,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                              if (msg instanceof Http3HeadersFrame) {
                                Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                if ("200".equals(headersFrame.headers().status().toString())) {
                                  connectStream[0] = (QuicStreamChannel) ctx.channel();
                                  handshakeLatch.countDown();
                                }
                              }
                            }
                          });
                }
              })
          .addListener(
              (Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                  Http3Headers headers = new DefaultHttp3Headers();
                  headers.method("CONNECT");
                  headers.scheme("https");
                  headers.path("/test-integration");
                  headers.authority("localhost");
                  headers.set(":protocol", "webtransport");
                  QuicStreamChannel ch = f.getNow();
                  ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
                }
              });

      assertTrue("Handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
      // Send WT_STREAMS_BLOCKED capsule to update the limit to max_absolute_streams.bidi=2
      WebTransportUtils.sendStreamsBlockedCapsule(connectStream[0], true, 10);
      Thread.sleep(200);

      // 1st stream creation: should succeed
      Future<QuicStreamChannel> s1 = quicClient.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel ch) {}
      });
      assertTrue("1st stream creation should succeed", s1.await(2, TimeUnit.SECONDS) && s1.isSuccess());

      // 2nd stream creation: should succeed
      Future<QuicStreamChannel> s2 = quicClient.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel ch) {}
      });
      assertTrue("2nd stream creation should succeed", s2.await(2, TimeUnit.SECONDS) && s2.isSuccess());

      // Write stream headers to consume limits
      io.netty.buffer.ByteBuf header1 = s1.getNow().alloc().buffer();
      WebTransportUtils.writeVarInt(header1, 0x41);
      WebTransportUtils.writeVarInt(header1, connectStream[0].streamId());
      s1.getNow().writeAndFlush(header1).await(2, TimeUnit.SECONDS);

      io.netty.buffer.ByteBuf header2 = s2.getNow().alloc().buffer();
      WebTransportUtils.writeVarInt(header2, 0x41);
      WebTransportUtils.writeVarInt(header2, connectStream[0].streamId());
      s2.getNow().writeAndFlush(header2).await(2, TimeUnit.SECONDS);

      // 3rd stream creation: client local QUIC allows it
      Future<QuicStreamChannel> s3 = quicClient.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel ch) {}
      });
      assertTrue("3rd stream QUIC creation should succeed locally", s3.await(2, TimeUnit.SECONDS) && s3.isSuccess());
 
      // Write stream header to s3: triggers server WebTransport limit enforcement
      io.netty.buffer.ByteBuf header3 = s3.getNow().alloc().buffer();
      WebTransportUtils.writeVarInt(header3, 0x41);
      WebTransportUtils.writeVarInt(header3, connectStream[0].streamId());
      s3.getNow().writeAndFlush(header3).await(2, TimeUnit.SECONDS);
 
      // The server must reject it and close the session (which triggers onSessionClosed)
      assertTrue("Session should be closed by server due to WebTransport flow control limit",
          sessionCloseLatch[0].await(5, TimeUnit.SECONDS));

      quicClient.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testDynamicSessionResumptionTimeoutIntegration() throws Exception {
    QuicSslContext clientSslContext =
        QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();

    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    clientSettings.put(0x2c7cf000L, 1L); // wt_enabled
    clientSettings.put(0x2b64L, 10L);
    clientSettings.put(0x2b65L, 10L);
    clientSettings.put(0x2b61L, 10000L);

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      // 1. Set resumption timeout to 1 second (expires almost instantly)
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.session.resumption.timeout.seconds=1"
      ));
      WebTransportConfig.reload();

      QuicChannel quicClient = bootstrap.connect().sync().getNow();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      final String[] resumptionToken = new String[1];

      Http3.newRequestStream(
              quicClient,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                              if (msg instanceof Http3HeadersFrame) {
                                Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                CharSequence token = headersFrame.headers().get("sec-webtransport-resumption-token");
                                if (token != null) {
                                  resumptionToken[0] = token.toString();
                                }
                                if ("200".equals(headersFrame.headers().status().toString())) {
                                  handshakeLatch.countDown();
                                }
                              }
                            }
                          });
                }
              })
          .addListener(
              (Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                  Http3Headers headers = new DefaultHttp3Headers();
                  headers.method("CONNECT");
                  headers.scheme("https");
                  headers.path("/test-integration");
                  headers.authority("localhost");
                  headers.set(":protocol", "webtransport");
                  headers.set("webtransport-resumption-token", "request-new-token");
                  QuicStreamChannel ch = f.getNow();
                  ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
                }
              });

      assertTrue("Initial handshake failed", handshakeLatch.await(5, TimeUnit.SECONDS));
      assertNotNull("Resumption token should not be null", resumptionToken[0]);

      // 2. Disconnect first client connection
      quicClient.close().sync();

      // 3. Wait 1.5 seconds so that the orphaned session is expired under the 1 second limit
      Thread.sleep(1500);

      // 4. Try to connect and resume on a new connection: should fail to resume
      ChannelHandler clientCodec2 =
          Http3.newQuicClientCodecBuilder()
              .sslContext(clientSslContext)
              .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
              .initialMaxData(10000000)
              .initialMaxStreamDataBidirectionalLocal(1000000)
              .initialMaxStreamDataBidirectionalRemote(1000000)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(100)
              .datagram(10000, 10000)
              .build();

      Channel clientChannel2 =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(clientCodec2)
              .bind(0)
              .sync()
              .channel();

      QuicChannelBootstrap bootstrap2 = QuicChannel.newBootstrap(clientChannel2)
          .handler(
              new ChannelInitializer<QuicChannel>() {
                @Override
                protected void initChannel(QuicChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new Http3ClientConnectionHandler(
                              new ChannelInitializer<QuicStreamChannel>() {
                                @Override
                                protected void initChannel(QuicStreamChannel stream) {}
                              },
                              (streamType) -> null,
                              (streamType) -> null,
                              new DefaultHttp3SettingsFrame(clientSettings),
                              false,
                              (id, value) -> true));
                }
              })
          .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      QuicChannel quicClient2 = bootstrap2.connect().sync().getNow();

      CountDownLatch handshakeLatch2 = new CountDownLatch(1);
      final String[] resumptionStatus = new String[1];
      final String[] newResumptionToken = new String[1];

      Http3.newRequestStream(
              quicClient2,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                              if (msg instanceof Http3HeadersFrame) {
                                Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                resumptionStatus[0] = headersFrame.headers().status().toString();
                                CharSequence token = headersFrame.headers().get("sec-webtransport-resumption-token");
                                newResumptionToken[0] = token != null ? token.toString() : null;
                                handshakeLatch2.countDown();
                              }
                            }
                          });
                }
              })
          .addListener(
              (Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                  Http3Headers headers = new DefaultHttp3Headers();
                  headers.method("CONNECT");
                  headers.scheme("https");
                  headers.path("/test-integration");
                  headers.authority("localhost");
                  headers.set(":protocol", "webtransport");
                  headers.set("webtransport-resumption-token", resumptionToken[0]);
                  QuicStreamChannel ch = f.getNow();
                  ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
                } else {
                  System.err.println("❌ SECOND HANDSHAKE REQUEST STREAM FUTURE FAILED: " + f.cause());
                }
              });

      if (!handshakeLatch2.await(5, TimeUnit.SECONDS)) {
        org.junit.Assert.fail("Second handshake failed. quicClient2 active: " + quicClient2.isActive());
      }
      org.junit.Assert.assertEquals("200", resumptionStatus[0]);
      org.junit.Assert.assertNotNull(newResumptionToken[0]);
      org.junit.Assert.assertNotEquals(resumptionToken[0], newResumptionToken[0]);

      quicClient2.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
    }
  }

  @Test
  public void testInvalidSessionResumptionFallbackIntegration() throws Exception {
    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();
    Http3Settings clientSetting = new Http3Settings((id, value) -> true);
    clientSetting.enableConnectProtocol(true);
    clientSetting.enableH3Datagram(true);
    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSetting),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final String[] responseStatus = new String[1];
    final String[] newResumptionToken = new String[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              responseStatus[0] = headersFrame.headers().status().toString();
                              CharSequence token = headersFrame.headers().get("sec-webtransport-resumption-token");
                              if (token != null) {
                                newResumptionToken[0] = token.toString();
                              }
                              handshakeLatch.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                // Pass an invalid / non-existent token
                headers.set("webtransport-resumption-token", "invalid-token-uuid-12345");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertEquals("200", responseStatus[0]);
    assertNotNull("A new resumption token should be generated on fallback", newResumptionToken[0]);

    quicClient.close().sync();
  }

  @Test
  public void testOriginValidationRejectionIntegration() throws Exception {
    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.webtransport.allowed_origins=https://trusted.com"
      ));
      WebTransportConfig.reload();

      tearDown();
      setUp();

      ChannelHandler clientCodec =
          Http3.newQuicClientCodecBuilder()
              .sslContext(clientSslContext)
              .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
              .initialMaxData(10000000)
              .initialMaxStreamDataBidirectionalLocal(1000000)
              .initialMaxStreamDataBidirectionalRemote(1000000)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(100)
              .datagram(10000, 10000)
              .build();

      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(clientCodec)
              .bind(0)
              .sync()
              .channel();
      Http3Settings clientSetting = new Http3Settings((id, value) -> true);
      clientSetting.enableConnectProtocol(true);
      clientSetting.enableH3Datagram(true);
      QuicChannelBootstrap bootstrap =
          QuicChannel.newBootstrap(clientChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new Http3ClientConnectionHandler(
                                  new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {}
                                  },
                                  (streamType) -> null,
                                  (streamType) -> null,
                                  new DefaultHttp3SettingsFrame(clientSetting),
                                  false,
                                  (id, value) -> true));
                    }
                  })
              .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      QuicChannel quicClient = bootstrap.connect().sync().getNow();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      final String[] responseStatus = new String[1];

      Http3.newRequestStream(
              quicClient,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                              if (msg instanceof Http3HeadersFrame) {
                                Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                responseStatus[0] = headersFrame.headers().status().toString();
                                handshakeLatch.countDown();
                              }
                            }
                          });
                }
              })
          .addListener(
              (Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                  Http3Headers headers = new DefaultHttp3Headers();
                  headers.method("CONNECT");
                  headers.scheme("https");
                  headers.path("/test-integration");
                  headers.authority("localhost");
                  headers.set(":protocol", "webtransport");
                  headers.set("origin", "https://untrusted-attacker.com");
                  QuicStreamChannel ch = f.getNow();
                  ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
                }
              });

      assertTrue("Handshake timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
      assertEquals("403", responseStatus[0]);

      quicClient.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
      tearDown();
      setUp();
    }
  }

  @Test
  public void testSessionKeepAliveTimeoutReapingIntegration() throws Exception {
    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.server.keepalive.enabled=true",
          "webtransport4j.server.keepalive.timeout.secs=1",
          "webtransport4j.server.keepalive.interval.secs=1"
      ));
      WebTransportConfig.reload();

      tearDown();
      setUp();

      ChannelHandler clientCodec =
          Http3.newQuicClientCodecBuilder()
              .sslContext(clientSslContext)
              .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
              .initialMaxData(10000000)
              .initialMaxStreamDataBidirectionalLocal(1000000)
              .initialMaxStreamDataBidirectionalRemote(1000000)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(100)
              .datagram(10000, 10000)
              .build();

      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(clientCodec)
              .bind(0)
              .sync()
              .channel();
      Http3Settings clientSetting = new Http3Settings((id, value) -> true);
      clientSetting.enableConnectProtocol(true);
      clientSetting.enableH3Datagram(true);
      QuicChannelBootstrap bootstrap =
          QuicChannel.newBootstrap(clientChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new Http3ClientConnectionHandler(
                                  new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {}
                                  },
                                  (streamType) -> null,
                                  (streamType) -> null,
                                  new DefaultHttp3SettingsFrame(clientSetting),
                                  false,
                                  (id, value) -> true));
                    }
                  })
              .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      QuicChannel quicClient = bootstrap.connect().sync().getNow();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      sessionCloseLatch[0] = new CountDownLatch(1);

      Http3.newRequestStream(
              quicClient,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                              if (msg instanceof Http3HeadersFrame) {
                                Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                if ("200".equals(headersFrame.headers().status().toString())) {
                                  handshakeLatch.countDown();
                                }
                              }
                            }
                          });
                }
              })
          .addListener(
              (Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                  Http3Headers headers = new DefaultHttp3Headers();
                  headers.method("CONNECT");
                  headers.scheme("https");
                  headers.path("/test-integration");
                  headers.authority("localhost");
                  headers.set(":protocol", "webtransport");
                  QuicStreamChannel ch = f.getNow();
                  ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
                }
              });

      assertTrue("Handshake timed out", handshakeLatch.await(5, TimeUnit.SECONDS));

      assertTrue("Session should be closed/reaped by server keep-alive",
          sessionCloseLatch[0].await(5, TimeUnit.SECONDS));

      quicClient.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
      tearDown();
      setUp();
    }
  }

  @Test
  public void testLargeDatagramTransferIntegration() throws Exception {
    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(65535, 65535)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    final CountDownLatch datagramAckLatch = new CountDownLatch(1);
    final String[] receivedAck = new String[1];
    Http3Settings clientSetting = new Http3Settings((id, value) -> true);
    clientSetting.enableConnectProtocol(true);
    clientSetting.enableH3Datagram(true);
    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSetting),
                                false,
                                (id, value) -> true));
                    ch.pipeline().addLast(new WebTransportDatagramDecoder());
                    ch.pipeline().addLast(
                        new SimpleChannelInboundHandler<WebTransportDatagramFrame>() {
                          @Override
                          protected void channelRead0(
                              ChannelHandlerContext ctx, WebTransportDatagramFrame msg) {
                            ByteBuf content = msg.content();
                            byte[] payload = new byte[content.readableBytes()];
                            content.readBytes(payload);
                            receivedAck[0] = new String(payload, StandardCharsets.UTF_8);
                            datagramAckLatch.countDown();
                          }
                        });
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Handshake timed out", handshakeLatch.await(5, TimeUnit.SECONDS));

    byte[] largePayloadBytes = new byte[1000];
    java.util.Arrays.fill(largePayloadBytes, (byte) 'A');

    ByteBuf payloadBuf = quicClient.alloc().buffer();
    WebTransportUtils.writeVarInt(payloadBuf, connectStream[0].streamId());
    payloadBuf.writeBytes(largePayloadBytes);

    quicClient.writeAndFlush(payloadBuf);

    assertTrue("Datagram ACK timed out", datagramAckLatch.await(5, TimeUnit.SECONDS));
    assertTrue("Ack should contain original content prefix",
        receivedAck[0].startsWith("ACK DG: I received the message from /test-integration: AAAAA"));

    quicClient.close().sync();
  }

  @Test
  public void testMalformedCapsuleRejectionIntegration() throws Exception {
    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();
    Http3Settings clientSetting = new Http3Settings((id, value) -> true);
    clientSetting.enableConnectProtocol(true);
    clientSetting.enableH3Datagram(true);
    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSetting),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Handshake timed out", handshakeLatch.await(5, TimeUnit.SECONDS));

    ByteBuf malformedFrame = connectStream[0].alloc().buffer();
    WebTransportUtils.writeVarInt(malformedFrame, 0x00);
    WebTransportUtils.writeVarInt(malformedFrame, 10);
    malformedFrame.writeByte(0x19);
    malformedFrame.writeByte(0x0B);

    connectStream[0].writeAndFlush(malformedFrame).await(2, TimeUnit.SECONDS);
    connectStream[0].shutdownOutput();

    assertTrue("CONNECT stream should close due to malformed capsule",
        connectStream[0].closeFuture().await(5, TimeUnit.SECONDS));

    quicClient.close().sync();
  }

  @Test
  public void testUnidirectionalStreamLifecycleIntegration() throws Exception {
    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();
    Http3Settings clientSetting = new Http3Settings((id, value) -> true);
    clientSetting.enableConnectProtocol(true);
    clientSetting.enableH3Datagram(true);
    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                new UnknownStreamHandlerFactory(),
                                new UnknownStreamHandlerFactory(),
                                new DefaultHttp3SettingsFrame(clientSetting),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Handshake timed out", handshakeLatch.await(5, TimeUnit.SECONDS));

    Future<QuicStreamChannel> uniStreamFuture = quicClient.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
      @Override protected void initChannel(QuicStreamChannel ch) {}
    });
    assertTrue("Uni stream creation should succeed", uniStreamFuture.await(2, TimeUnit.SECONDS) && uniStreamFuture.isSuccess());

    QuicStreamChannel uniStream = uniStreamFuture.getNow();
    ByteBuf payload = uniStream.alloc().buffer();
    WebTransportUtils.writeVarInt(payload, 0x54);
    WebTransportUtils.writeVarInt(payload, connectStream[0].streamId());
    payload.writeBytes("Hello Uni Stream".getBytes(StandardCharsets.UTF_8));
    uniStream.writeAndFlush(payload).await(2, TimeUnit.SECONDS);

    assertFalse("CONNECT stream should remain active", connectStream[0].closeFuture().isDone());

    Future<QuicStreamChannel> badUniStreamFuture = quicClient.createStream(QuicStreamType.UNIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
      @Override protected void initChannel(QuicStreamChannel ch) {
        ch.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
          @Override
          public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
          }
        });
      }
    });
    assertTrue("Bad Uni stream creation should succeed", badUniStreamFuture.await(2, TimeUnit.SECONDS) && badUniStreamFuture.isSuccess());

    QuicStreamChannel badUniStream = badUniStreamFuture.getNow();
    ByteBuf badPayload = badUniStream.alloc().buffer();
    WebTransportUtils.writeVarInt(badPayload, 0x41);
    WebTransportUtils.writeVarInt(badPayload, connectStream[0].streamId());
    badPayload.writeBytes("Boom".getBytes(StandardCharsets.UTF_8));
    badUniStream.writeAndFlush(badPayload).await(2, TimeUnit.SECONDS);
    badUniStream.shutdownOutput();

    assertTrue("Bad Uni stream should be closed by server due to incorrect type prefix",
        badUniStream.closeFuture().await(5, TimeUnit.SECONDS));
    assertFalse("CONNECT stream should remain active", connectStream[0].closeFuture().isDone());

    quicClient.close().sync();
  }

  @Test
  public void testResumptionTokenReusePreventionIntegration() throws Exception {
    ChannelHandler clientCodec1 =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec1)
            .bind(0)
            .sync()
            .channel();
    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);
    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient = bootstrap.connect().sync().getNow();
    CountDownLatch handshakeLatch1 = new CountDownLatch(1);
    final String[] token = new String[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              CharSequence tokenSeq = headersFrame.headers().get("sec-webtransport-resumption-token");
                              if (tokenSeq != null) {
                                token[0] = tokenSeq.toString();
                              }
                              handshakeLatch1.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch1.await(5, TimeUnit.SECONDS));
    assertNotNull(token[0]);
    quicClient.close().sync();

    ChannelHandler clientCodec2 =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel2 =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec2)
            .bind(0)
            .sync()
            .channel();
    Http3Settings clientSetting = new Http3Settings((id, value) -> true);
    clientSetting.enableConnectProtocol(true);
    clientSetting.enableH3Datagram(true);
    QuicChannelBootstrap bootstrap2 =
        QuicChannel.newBootstrap(clientChannel2)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient2 = bootstrap2.connect().sync().getNow();
    CountDownLatch handshakeLatch2 = new CountDownLatch(1);
    final String[] status2 = new String[1];
    final String[] token2 = new String[1];

    Http3.newRequestStream(
            quicClient2,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              status2[0] = headersFrame.headers().status().toString();
                              CharSequence tokenSeq = headersFrame.headers().get("sec-webtransport-resumption-token");
                              if (tokenSeq != null) {
                                token2[0] = tokenSeq.toString();
                              }
                              handshakeLatch2.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                headers.set("webtransport-resumption-token", token[0]);
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch2.await(5, TimeUnit.SECONDS));
    assertEquals("200", status2[0]);
    assertNotNull(token2[0]);
    quicClient2.close().sync();

    ChannelHandler clientCodec3 =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel3 =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec3)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap3 =
        QuicChannel.newBootstrap(clientChannel3)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient3 = bootstrap3.connect().sync().getNow();
    CountDownLatch handshakeLatch3 = new CountDownLatch(1);
    final String[] status3 = new String[1];
    final String[] token3 = new String[1];

    Http3.newRequestStream(
            quicClient3,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              status3[0] = headersFrame.headers().status().toString();
                              CharSequence tokenSeq = headersFrame.headers().get("sec-webtransport-resumption-token");
                              if (tokenSeq != null) {
                                token3[0] = tokenSeq.toString();
                              }
                              handshakeLatch3.countDown();
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                headers.set("webtransport-resumption-token", token[0]);
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch3.await(5, TimeUnit.SECONDS));
    assertEquals("200", status3[0]);
    assertNotNull(token3[0]);
    org.junit.Assert.assertNotEquals(token[0], token3[0]);

    quicClient3.close().sync();
  }

  @Test
  public void testUnknownCapsuleTypeIgnoredIntegration() throws Exception {
    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();
    Http3Settings clientSetting = new Http3Settings((id, value) -> true);
    clientSetting.enableConnectProtocol(true);
    clientSetting.enableH3Datagram(true);
    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSetting),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));

    QuicChannel quicClient = bootstrap.connect().sync().getNow();

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                connectStream[0] = (QuicStreamChannel) ctx.channel();
                                handshakeLatch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("Handshake timed out", handshakeLatch.await(5, TimeUnit.SECONDS));

    WebTransportUtils.sendStreamsBlockedCapsule(connectStream[0], true, 10);
    
    ByteBuf unknownCapsule = connectStream[0].alloc().buffer();
    WebTransportUtils.writeVarInt(unknownCapsule, 0x999999L);
    WebTransportUtils.writeVarInt(unknownCapsule, 5);
    unknownCapsule.writeBytes("Dummy".getBytes(StandardCharsets.UTF_8));
    connectStream[0].writeAndFlush(new DefaultHttp3DataFrame(unknownCapsule)).await(2, TimeUnit.SECONDS);

    Thread.sleep(300);
    assertFalse("CONNECT stream should remain open since unknown capsules must be ignored",
        connectStream[0].closeFuture().isDone());

    Future<QuicStreamChannel> s1 = quicClient.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
      @Override protected void initChannel(QuicStreamChannel ch) {}
    });
    assertTrue("Bidi stream creation should succeed", s1.await(2, TimeUnit.SECONDS) && s1.isSuccess());

    quicClient.close().sync();
  }

  @Test
  public void testMaxSessionsPerConnectionEnforcementIntegration() throws Exception {
    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.webtransport.max_sessions_per_connection=2"
      ));
      WebTransportConfig.reload();

      tearDown();
      setUp();

      ChannelHandler clientCodec =
          Http3.newQuicClientCodecBuilder()
              .sslContext(clientSslContext)
              .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
              .initialMaxData(10000000)
              .initialMaxStreamDataBidirectionalLocal(1000000)
              .initialMaxStreamDataBidirectionalRemote(1000000)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(100)
              .datagram(10000, 10000)
              .build();

      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(clientCodec)
              .bind(0)
              .sync()
              .channel();
Http3Settings clientSettings = new Http3Settings((id, value) -> true);
clientSettings.enableConnectProtocol(true);
clientSettings.enableH3Datagram(true);
      QuicChannelBootstrap bootstrap =
          QuicChannel.newBootstrap(clientChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new Http3ClientConnectionHandler(
                                  new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {}
                                  },
                                  (streamType) -> null,
                                  (streamType) -> null,
                                  new DefaultHttp3SettingsFrame(clientSettings),
                                  false,
                                  (id, value) -> true));
                    }
                  })
              .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      QuicChannel quicClient = bootstrap.connect().sync().getNow();

      CountDownLatch handshakeLatch1 = new CountDownLatch(1);
      final String[] status1 = new String[1];
      Http3.newRequestStream(quicClient, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel ch) {
          ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
              if (msg instanceof Http3HeadersFrame) {
                status1[0] = ((Http3HeadersFrame) msg).headers().status().toString();
                handshakeLatch1.countDown();
              }
            }
          });
        }
      }).addListener((Future<QuicStreamChannel> f) -> {
        if (f.isSuccess()) {
          Http3Headers headers = new DefaultHttp3Headers();
          headers.method("CONNECT").scheme("https").path("/test-integration").authority("localhost").set(":protocol", "webtransport");
          f.getNow().writeAndFlush(new DefaultHttp3HeadersFrame(headers));
        }
      });
      assertTrue(handshakeLatch1.await(5, TimeUnit.SECONDS));
      assertEquals("200", status1[0]);

      CountDownLatch handshakeLatch2 = new CountDownLatch(1);
      final String[] status2 = new String[1];
      Http3.newRequestStream(quicClient, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel ch) {
          ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
              if (msg instanceof Http3HeadersFrame) {
                status2[0] = ((Http3HeadersFrame) msg).headers().status().toString();
                handshakeLatch2.countDown();
              }
            }
          });
        }
      }).addListener((Future<QuicStreamChannel> f) -> {
        if (f.isSuccess()) {
          Http3Headers headers = new DefaultHttp3Headers();
          headers.method("CONNECT").scheme("https").path("/test-integration").authority("localhost").set(":protocol", "webtransport");
          f.getNow().writeAndFlush(new DefaultHttp3HeadersFrame(headers));
        }
      });
      assertTrue(handshakeLatch2.await(5, TimeUnit.SECONDS));
      assertEquals("200", status2[0]);

      CountDownLatch handshakeLatch3 = new CountDownLatch(1);
      final String[] status3 = new String[1];
      Http3.newRequestStream(quicClient, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel ch) {
          ch.pipeline().addLast(new SimpleChannelInboundHandler<Object>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
              if (msg instanceof Http3HeadersFrame) {
                status3[0] = ((Http3HeadersFrame) msg).headers().status().toString();
                handshakeLatch3.countDown();
              }
            }
          });
        }
      }).addListener((Future<QuicStreamChannel> f) -> {
        if (f.isSuccess()) {
          Http3Headers headers = new DefaultHttp3Headers();
          headers.method("CONNECT").scheme("https").path("/test-integration").authority("localhost").set(":protocol", "webtransport");
          f.getNow().writeAndFlush(new DefaultHttp3HeadersFrame(headers));
        }
      });
      assertTrue(handshakeLatch3.await(5, TimeUnit.SECONDS));
      assertEquals("429", status3[0]);

      quicClient.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
      tearDown();
      setUp();
    }
  }

  @Test
  public void testDynamicTrafficShapingReloadIntegration() throws Exception {
    java.io.File tempFile = new java.io.File("webtransport-dynamic.properties");
    try {
      java.nio.file.Files.write(tempFile.toPath(), java.util.Arrays.asList(
          "webtransport4j.server.traffic.connection.read.limit=2048"
      ));
      WebTransportConfig.reload();

      ChannelHandler clientCodec =
          Http3.newQuicClientCodecBuilder()
              .sslContext(clientSslContext)
              .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
              .initialMaxData(10000000)
              .initialMaxStreamDataBidirectionalLocal(1000000)
              .initialMaxStreamDataBidirectionalRemote(1000000)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(100)
              .datagram(10000, 10000)
              .build();

      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(clientCodec)
              .bind(0)
              .sync()
              .channel();
Http3Settings clientSetting = new Http3Settings((id, value) -> true);
clientSetting.enableH3Datagram(true);
clientSetting.enableConnectProtocol(true);
      QuicChannelBootstrap bootstrap =
          QuicChannel.newBootstrap(clientChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new Http3ClientConnectionHandler(
                                  new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {}
                                  },
                                  (streamType) -> null,
                                  (streamType) -> null,
                                  new DefaultHttp3SettingsFrame(clientSetting),
                                  false,
                                  (id, value) -> true));
                    }
                  })
              .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      QuicChannel quicClient = bootstrap.connect().sync().getNow();

      CountDownLatch handshakeLatch = new CountDownLatch(1);
      final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

      Http3.newRequestStream(
              quicClient,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                              if (msg instanceof Http3HeadersFrame) {
                                Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                if ("200".equals(headersFrame.headers().status().toString())) {
                                  connectStream[0] = (QuicStreamChannel) ctx.channel();
                                  handshakeLatch.countDown();
                                }
                              }
                            }
                          });
                }
              })
          .addListener(
              (Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                  Http3Headers headers = new DefaultHttp3Headers();
                  headers.method("CONNECT");
                  headers.scheme("https");
                  headers.path("/test-integration");
                  headers.authority("localhost");
                  headers.set(":protocol", "webtransport");
                  QuicStreamChannel ch = f.getNow();
                  ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
                }
              });

      assertTrue("Handshake timed out", handshakeLatch.await(5, TimeUnit.SECONDS));

      CountDownLatch bidiLatch = new CountDownLatch(1);
      final int[] totalBytesReceived = new int[1];

      Future<QuicStreamChannel> s1Future = quicClient.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
        @Override protected void initChannel(QuicStreamChannel ch) {
          ch.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
              ctx.channel().eventLoop().execute(() -> {
                java.util.List<String> toRemove = new java.util.ArrayList<>();
                for (String name : ctx.pipeline().names()) {
                  ChannelHandler h = ctx.pipeline().get(name);
                  if (h != null && h != this && (name.contains("Http3") || h.getClass().getName().contains("Http3"))) {
                    toRemove.add(name);
                  }
                }
                for (String name : toRemove) {
                  try { ctx.pipeline().remove(name); } catch (Exception e) {}
                }
              });
              super.handlerAdded(ctx);
            }
          });
          ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
              totalBytesReceived[0] += msg.readableBytes();
              if (totalBytesReceived[0] >= 6144) {
                bidiLatch.countDown();
              }
            }
          });
        }
      });
      assertTrue("Bidi stream creation should succeed", s1Future.await(2, TimeUnit.SECONDS) && s1Future.isSuccess());
      QuicStreamChannel s1 = s1Future.getNow();

      ByteBuf header = s1.alloc().buffer();
      WebTransportUtils.writeVarInt(header, 0x41);
      WebTransportUtils.writeVarInt(header, connectStream[0].streamId());
      s1.writeAndFlush(header).await(2, TimeUnit.SECONDS);

      byte[] payload = new byte[6144];
      java.util.Arrays.fill(payload, (byte) 'A');
      
      long startTime = System.currentTimeMillis();
      s1.writeAndFlush(io.netty.buffer.Unpooled.copiedBuffer(payload));
      
      assertTrue("Echo data transfer timed out", bidiLatch.await(10, TimeUnit.SECONDS));
      long duration = System.currentTimeMillis() - startTime;

      assertTrue("Data transfer should have been throttled (duration=" + duration + "ms)", duration > 1000);

      quicClient.close().sync();

    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      WebTransportConfig.reload();
    }
  }

  private boolean tryConnectAndVerifyActive(int port) {
    try {
      ChannelHandler clientCodec =
          Http3.newQuicClientCodecBuilder()
              .sslContext(clientSslContext)
              .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
              .initialMaxData(10000000)
              .initialMaxStreamDataBidirectionalLocal(1000000)
              .initialMaxStreamDataBidirectionalRemote(1000000)
              .initialMaxStreamsBidirectional(100)
              .initialMaxStreamsUnidirectional(100)
              .datagram(10000, 10000)
              .build();

      Channel clientChannel =
          new Bootstrap()
              .group(clientGroup)
              .channel(NioDatagramChannel.class)
              .handler(clientCodec)
              .bind(0)
              .sync()
              .channel();

      Http3Settings clientSetting = new Http3Settings((id, value) -> true);
      clientSetting.enableConnectProtocol(true);
      clientSetting.enableH3Datagram(true);

      QuicChannelBootstrap bootstrap =
          QuicChannel.newBootstrap(clientChannel)
              .handler(
                  new ChannelInitializer<QuicChannel>() {
                    @Override
                    protected void initChannel(QuicChannel ch) {
                      ch.pipeline()
                          .addLast(
                              new Http3ClientConnectionHandler(
                                  new ChannelInitializer<QuicStreamChannel>() {
                                    @Override
                                    protected void initChannel(QuicStreamChannel stream) {}
                                  },
                                  new UnknownStreamHandlerFactory(),
                                  new UnknownStreamHandlerFactory(),
                                  new DefaultHttp3SettingsFrame(clientSetting),
                                  false,
                                  (id, value) -> true));
                    }
                  })
              .remoteAddress(new InetSocketAddress("127.0.0.1", port));

      Future<QuicChannel> connectFuture = bootstrap.connect();
      if (!connectFuture.await(2, TimeUnit.SECONDS)) {
        clientChannel.close();
        return false;
      }
      if (!connectFuture.isSuccess()) {
        clientChannel.close();
        return false;
      }
      QuicChannel quicClient = connectFuture.getNow();
      boolean closed = quicClient.closeFuture().await(500, TimeUnit.MILLISECONDS);
      quicClient.close();
      clientChannel.close();
      return !closed;
    } catch (Exception e) {
      return false;
    }
  }

  @Test
  public void testIpRateLimitingAndBlocklistIntegration() throws Exception {
    java.lang.reflect.Field field = IpRateLimitingHandler.class.getDeclaredField("ipCounts");
    field.setAccessible(true);
    java.util.Map<?, ?> ipCounts = (java.util.Map<?, ?>) field.get(null);
    ipCounts.clear();

    System.setProperty("webtransport4j.server.ratelimit.whitelist", "");
    System.setProperty("webtransport4j.server.ratelimit.blocklist", "127.0.0.1");
    WebTransportConfig.reload();
    IpRateLimitingHandler.reloadSharedConfig();

    try {
      assertFalse("Connection from blocked IP should be rejected", tryConnectAndVerifyActive(port));

      System.clearProperty("webtransport4j.server.ratelimit.blocklist");
      System.setProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute", "2");
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();
      ipCounts.clear();

      assertTrue("First connection should be accepted", tryConnectAndVerifyActive(port));
      assertTrue("Second connection should be accepted", tryConnectAndVerifyActive(port));
      assertFalse("Third connection exceeding limit should be rejected", tryConnectAndVerifyActive(port));

    } finally {
      System.clearProperty("webtransport4j.server.ratelimit.whitelist");
      System.clearProperty("webtransport4j.server.ratelimit.blocklist");
      System.clearProperty("webtransport4j.server.ratelimit.max_connections_per_ip_per_minute");
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();
      ipCounts.clear();
    }
  }

  @Test
  public void testBackgroundConfigReloaderIntegration() throws Exception {
    Field field = IpRateLimitingHandler.class.getDeclaredField("ipCounts");
    field.setAccessible(true);
    Map<?, ?> ipCounts = (Map<?, ?>) field.get(null);
    ipCounts.clear();

    // Verify localhost is normally accepted
    File tempFile = new File("webtransport-dynamic.properties");
    if (tempFile.exists()) {
      tempFile.delete();
    }
    System.clearProperty("webtransport4j.server.ratelimit.blocklist");
    System.setProperty("webtransport4j.server.ratelimit.whitelist", "");
    WebTransportConfig.reload();
    IpRateLimitingHandler.reloadSharedConfig();

    assertTrue("Normally localhost should connect", tryConnectAndVerifyActive(port));

    // Now write blocklist to properties file to simulate dynamic file modification
    Files.write(tempFile.toPath(), Arrays.asList(
        "webtransport4j.server.ratelimit.blocklist=127.0.0.1",
        "webtransport4j.server.ratelimit.whitelist="
    ));

    // Wait for the background thread to trigger (runs every 10 seconds, so wait 12 seconds to be safe)
    log.info("Waiting 12 seconds for the background wt-rate-limit-reloader thread to trigger...");
    Thread.sleep(12000);

    try {
      assertFalse("After background reload, 127.0.0.1 connection should be rejected by blocklist",
          tryConnectAndVerifyActive(port));
    } finally {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      System.clearProperty("webtransport4j.server.ratelimit.whitelist");
      System.clearProperty("webtransport4j.server.ratelimit.blocklist");
      WebTransportConfig.reload();
      IpRateLimitingHandler.reloadSharedConfig();
      ipCounts.clear();
    }
  }

  @Test
  public void testSessionResumptionDisabledIntegration() throws Exception {
    Http3Settings clientSettings = new Http3Settings((id, value) -> true);
    clientSettings.enableH3Datagram(true);
    clientSettings.enableConnectProtocol(true);

    ChannelHandler clientCodec =
        Http3.newQuicClientCodecBuilder()
            .sslContext(clientSslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .datagram(10000, 10000)
            .build();

    Channel clientChannel =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(clientCodec)
            .bind(0)
            .sync()
            .channel();

    QuicChannelBootstrap bootstrap =
        QuicChannel.newBootstrap(clientChannel)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    ch.pipeline()
                        .addLast(
                            new Http3ClientConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {}
                                },
                                (streamType) -> null,
                                (streamType) -> null,
                                new DefaultHttp3SettingsFrame(clientSettings),
                                false,
                                (id, value) -> true));
                  }
                })
            .remoteAddress(new InetSocketAddress("127.0.0.1", port));
    QuicChannel quicClient1 = bootstrap.connect().sync().getNow();

    CountDownLatch handshake1Latch = new CountDownLatch(1);
    final String[] tokenContainer = new String[1];
    final QuicStreamChannel[] connectStream1 = new QuicStreamChannel[1];

    Http3.newRequestStream(
            quicClient1,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              if ("200".equals(headersFrame.headers().status().toString())) {
                                CharSequence rawToken = headersFrame.headers().get("sec-webtransport-resumption-token");
                                if (rawToken != null) {
                                  tokenContainer[0] = rawToken.toString();
                                }
                                connectStream1[0] = (QuicStreamChannel) ctx.channel();
                                handshake1Latch.countDown();
                              }
                            }
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                QuicStreamChannel ch = f.getNow();
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("First handshake failed or timed out", handshake1Latch.await(5, TimeUnit.SECONDS));
    assertNotNull("Resumption token must be provided by server", tokenContainer[0]);

    // Close first connection to register the session as orphaned
    quicClient1.close().sync();

    // Sleep to let server register the session in resumption cache
    Thread.sleep(200);

    // Disable resumption on the server
    System.setProperty("webtransport4j.session.resumption.enabled", "false");
    WebTransportConfig.reload();

    try {
      // Connect second client presenting the token
      QuicChannel quicClient2 = bootstrap.connect().sync().getNow();
      CountDownLatch handshake2Latch = new CountDownLatch(1);
      final String[] returnedTokenContainer = new String[1];
      final QuicStreamChannel[] connectStream2 = new QuicStreamChannel[1];

      Http3.newRequestStream(
              quicClient2,
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                  ch.pipeline()
                      .addLast(
                          new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                              if (msg instanceof Http3HeadersFrame) {
                                Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                                if ("200".equals(headersFrame.headers().status().toString())) {
                                  CharSequence rawToken = headersFrame.headers().get("sec-webtransport-resumption-token");
                                  if (rawToken != null) {
                                    returnedTokenContainer[0] = rawToken.toString();
                                  }
                                  connectStream2[0] = (QuicStreamChannel) ctx.channel();
                                  handshake2Latch.countDown();
                                }
                              }
                            }
                          });
                }
              })
          .addListener(
              (Future<QuicStreamChannel> f) -> {
                if (f.isSuccess()) {
                  Http3Headers headers = new DefaultHttp3Headers();
                  headers.method("CONNECT");
                  headers.scheme("https");
                  headers.path("/test-integration");
                  headers.authority("localhost");
                  headers.set(":protocol", "webtransport");
                  headers.set("webtransport-resumption-token", tokenContainer[0]);
                  QuicStreamChannel ch = f.getNow();
                  ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
                }
              });

      assertTrue("Second handshake failed or timed out", handshake2Latch.await(5, TimeUnit.SECONDS));

      // Assert fallback to new session:
      // 1. No new token should be sent
      assertNull("No resumption token should be sent when resumption is disabled", returnedTokenContainer[0]);
      // 2. The connection channels are different
      assertNotEquals(quicClient1, quicClient2);

      quicClient2.close().sync();
    } finally {
      System.clearProperty("webtransport4j.session.resumption.enabled");
      WebTransportConfig.reload();
      clientChannel.close().sync();
    }
  }
}

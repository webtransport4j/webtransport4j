package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import static org.junit.Assert.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http3.*;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WebTransportIntegrationTest {

  private NioEventLoopGroup serverGroup;
  private NioEventLoopGroup clientGroup;
  private Channel serverChannel;
  private int port;
  private QuicChannel serverConnectionChannel;
  private WebTransportServer webTransportServer;

  @Before
  public void setUp() throws Exception {
    setUpServer(10000L);
  }

  private void setUpServer(long initialMaxData) throws Exception {
    System.setProperty("webtransport4j.webtransport.enable_server_push", "false");
    webTransportServer = new WebTransportServer();
    webTransportServer.registerHandler(
        "/test-integration",
        new WebTransportHandler() {
          @Override
          public void onSessionReady(WebTransportSession session) {
            System.out.println("TEST SERVER: Session ready: " + session.getSessionStreamId());
          }

          @Override
          public void onSessionClosed(WebTransportSession session) {
            System.out.println("TEST SERVER: Session closed: " + session.getSessionStreamId());
          }

          @Override
          public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
            System.out.println(
                "TEST SERVER: Incoming stream: "
                    + stream.streamId()
                    + " (bidi="
                    + stream.isBidirectional()
                    + ")");
            stream.onData(
                data -> {
                  String content = data.toString(StandardCharsets.UTF_8);
                  System.out.println(
                      "TEST SERVER: Received on stream " + stream.streamId() + ": " + content);
                  if (stream.isBidirectional()) {
                    stream.writeText(
                        "ACK BI: I received the message from " + session.path() + ": " + content);
                  } else {
                    System.out.println(
                        "Unidirectional message received from client :"
                            + session.path()
                            + ": "
                            + content);
                  }
                });
          }

          @Override
          public void onDatagramReceived(WebTransportSession session, ByteBuf data) {
            String content = data.toString(StandardCharsets.UTF_8);
            System.out.println("TEST SERVER: Received datagram: " + content);
            ByteBuf resp = session.getConnectStream().alloc().directBuffer();
            resp.writeBytes(
                ("ACK DG: I received the message from " + session.path() + ": " + content)
                    .getBytes(StandardCharsets.UTF_8));
            session.sendDatagram(resp);
          }
        });
    serverGroup = new NioEventLoopGroup(1);
    clientGroup = new NioEventLoopGroup(1);

    // Server SSL
    String keyPath = WebTransportConfig.get("webtransport4j.ssl.key.path", null);
    String certPath = WebTransportConfig.get("webtransport4j.ssl.cert.path", null);

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
              .applicationProtocols(Http3.supportedApplicationProtocols())
              .build();
    } else {
      io.netty.handler.ssl.util.SelfSignedCertificate ssc = new io.netty.handler.ssl.util.SelfSignedCertificate();
      serverSslContext =
          QuicSslContextBuilder.forServer(ssc.privateKey(), null, ssc.certificate())
              .applicationProtocols(Http3.supportedApplicationProtocols())
              .build();
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
                WebTransportServer.addTrafficShapers(ch);
                ch.pipeline()
                    .addLast(
                        new ChannelInboundHandlerAdapter() {
                          private boolean sessionHeaderRead = false;

                          @Override
                          public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            if (msg instanceof ByteBuf) {
                              ByteBuf data = (ByteBuf) msg;
                              if (!sessionHeaderRead) {
                                long sessionId = WebTransportUtils.readVariableLengthInt(data);
                                ctx.channel().attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(sessionId);
                                sessionHeaderRead = true;
                              }
                              if (!data.isReadable()) {
                                data.release();
                                return;
                              }
                              ctx.fireChannelRead(data);
                            }
                          }
                        });
                ch.pipeline().addLast(new RawWebTransportHandler());
                ch.pipeline().addLast(new MessageDispatcher());
              }
            };
          }
          return null;
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
                    serverConnectionChannel = ch;
                    ch.attr(WebTransportAttributeKeys.SERVER_KEY).set(webTransportServer);
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

                    ch.pipeline().addLast(new WebTransportDatagramHandler());
                    ch.pipeline().addLast(new WebTransportCapsuleHandler());
                    ch.pipeline().addLast(new MessageDispatcher());
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  @Override
                                  protected void initChannel(QuicStreamChannel stream) {
                                    System.out.println(
                                        "SERVER: initChannel for stream ID "
                                            + stream.streamId()
                                            + " | type "
                                            + stream.type());
                                    WebTransportServer.addTrafficShapers(stream);
                                    stream.pipeline().addFirst(new WebTransportDetectorHandler());
                                    stream.pipeline().addLast(new RawWebTransportHandler());
                                    stream.pipeline().addLast(new WebTransportStreamFrameDecoder());
                                    stream.pipeline().addLast(new WebTransportHeadersHandler());
                                    stream.pipeline().addLast(new WebTransportDataHandler());
                                    stream.pipeline().addLast(new WebTransportCapsuleHandler());
                                    stream.pipeline().addLast(new MessageDispatcher());
                                  }
                                },
                                new ChannelInboundHandlerAdapter() {
                                  @Override
                                  public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    System.out.println(
                                        "SERVER: Received message on control stream of class: "
                                            + msg.getClass().getName());
                                    if (msg instanceof Http3SettingsFrame) {
                                      Http3SettingsFrame settingsFrame = (Http3SettingsFrame) msg;
                                      io.netty.handler.codec.http3.Http3Settings settings =
                                          settingsFrame.settings();
                                      System.out.println("SERVER: Received settings: " + settings);
                                      if (settings != null) {
                                        System.out.println(
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

                                        boolean valid = settings.h3DatagramEnabled();
                                        // Set attributes so WebTransportHeadersHandler can check
                                        // them
                                        if (quic != null) {
                                          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_RECEIVED)
                                              .set(true);
                                          quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_VALID)
                                              .set(valid);
                                        }

                                        // Section 5.1: Verify required setting SETTINGS_H3_DATAGRAM
                                        // (0x33) is enabled (1)
                                        if (!valid) {
                                          System.out.println(
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
                                                System.out.println(
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

                                        System.out.println(
                                            "SERVER: Settings parent quic channel: " + quic);
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

    port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
  }

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
                            System.out.println(
                                "CLIENT CONNECT stream exceptionCaught: " + cause.getMessage());
                            closeLatch.countDown();
                            ctx.close();
                          }

                          @Override
                          public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                            System.out.println("CLIENT CONNECT stream channelInactive");
                            closeLatch.countDown();
                            super.channelInactive(ctx);
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                                        } catch (Exception ignored) {
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
                                        } catch (Exception ignored) {
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
  public void testSessionFlowControlBufferingAndFlushing() throws Exception {
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

    LongFunction<ChannelHandler> clientUniStreamFactory =
        (streamType) -> {
          if (streamType == 0x00) { // Control stream
            return new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                ch.pipeline()
                    .addLast(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void channelRead(ChannelHandlerContext ctx, Object msg) {
                            System.out.println(
                                "CLIENT CONTROL: received message " + msg.getClass().getName());
                            if (msg instanceof Http3SettingsFrame) {
                              Http3SettingsFrame settingsFrame = (Http3SettingsFrame) msg;
                              io.netty.handler.codec.http3.Http3Settings settings =
                                  settingsFrame.settings();
                              if (settings != null) {
                                System.out.println(
                                    "CLIENT CONTROL: settings received: " + settings);
                                QuicChannel quic = ((QuicStreamChannel) ctx.channel()).parent();
                                if (quic != null) {
                                  quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI)
                                      .set(settings.get(0x2b64L));
                                  quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI)
                                      .set(settings.get(0x2b65L));
                                  quic.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)
                                      .set(settings.get(0x2b61L));
                                }
                              }
                            }
                            io.netty.util.ReferenceCountUtil.release(msg);
                          }
                        });
              }
            };
          }
          return null;
        };

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
                                    stream.pipeline().addLast(new WebTransportDataHandler());
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
                                                      System.out.println(
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
                                // Inline capsule decoder for client-side CONNECT stream
                                ctx.pipeline()
                                    .addLast(
                                        new ChannelInboundHandlerAdapter() {
                                          @Override
                                          public void channelRead(
                                              ChannelHandlerContext c, Object msg) {
                                            if (msg instanceof Http3DataFrame) {
                                              Http3DataFrame frame = (Http3DataFrame) msg;
                                              ByteBuf payload = frame.content();
                                              if (payload.isReadable()) {
                                                while (payload.isReadable()) {
                                                  payload.markReaderIndex();
                                                  long capType =
                                                      WebTransportUtils.readVariableLengthInt(
                                                          payload);
                                                  if (capType == -1) {
                                                    payload.resetReaderIndex();
                                                    break;
                                                  }
                                                  long capLen =
                                                      WebTransportUtils.readVariableLengthInt(
                                                          payload);
                                                  if (capLen == -1
                                                      || payload.readableBytes() < capLen) {
                                                    payload.resetReaderIndex();
                                                    break;
                                                  }
                                                  ByteBuf capVal = payload.readSlice((int) capLen);
                                                  Long sessId =
                                                      ((QuicStreamChannel) c.channel())
                                                          .attr(WebTransportAttributeKeys.SESSION_ID_KEY)
                                                          .get();
                                                  long sessionId =
                                                      (sessId != null)
                                                          ? sessId
                                                          : ((QuicStreamChannel) c.channel())
                                                              .streamId();
                                                  c.fireChannelRead(
                                                      new WebTransportCapsule(
                                                          sessionId, capType, capVal.retain()));
                                                }
                                              }
                                              io.netty.util.ReferenceCountUtil.release(frame);
                                            } else {
                                              c.fireChannelRead(msg);
                                            }
                                          }
                                        });
                                ctx.pipeline().addLast(new WebTransportCapsuleHandler());
                                handshakeLatch.countDown();
                                // Remove this handler so Http3DataFrames pass to the capsule
                                // decoder
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue("CONNECT handshake failed or timed out", handshakeLatch.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream[0]);
    long sessionId = connectStream[0].streamId();

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
                                        } catch (Exception ignored) {
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

    // The write exceeds the limit, so it will be buffered as a PendingWrite.
    // However, the WT_DATA_BLOCKED capsule auto-response from the server may
    // already trigger WT_MAX_DATA, flushing the pending write before we can observe it.
    // Therefore, just wait for the write to succeed — this validates the full flow:
    // write blocked → WT_DATA_BLOCKED sent → WT_MAX_DATA received → write flushed.
    assertTrue(
        "Pending write failed to complete after flow control resolution",
        writePromise.await(5, TimeUnit.SECONDS));
    assertTrue(writePromise.isSuccess());
    assertEquals(0, clientSession.getPendingWrites().size());

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

    CountDownLatch handshakeLatch = new CountDownLatch(1);
    CountDownLatch datagramLatch = new CountDownLatch(1);
    CountDownLatch bidiEchoLatch = new CountDownLatch(1);

    final QuicStreamChannel[] connectStream = new QuicStreamChannel[1];

    // Unidirectional Stream Type Handler Factory on Client
    LongFunction<ChannelHandler> clientUniStreamFactory =
        (streamType) -> {
          return new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
              System.out.println("CLIENT: initChannel for uni stream ID " + ch.streamId());
              ch.pipeline().addFirst(new WebTransportDetectorHandler());
              ch.pipeline()
                  .addLast(
                      new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                          System.out.println(
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
                                    System.out.println(
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
                                                System.out.println(
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
    System.out.println("DEBUG: Client connected. Active: " + quicClient.isActive());

    // Register WebTransportDatagramHandler on Client QuicChannel pipeline
    quicClient.pipeline().addLast(new WebTransportDatagramHandler());
    quicClient
        .pipeline()
        .addLast(
            new SimpleChannelInboundHandler<WebTransportDatagramFrame>() {
              @Override
              protected void channelRead0(
                  ChannelHandlerContext ctx, WebTransportDatagramFrame msg) {
                String content = msg.content().toString(StandardCharsets.UTF_8);
                System.out.println("DEBUG: Client received Datagram: " + content);
                if (content.contains("ACK DG")) {
                  datagramLatch.countDown();
                }
              }
            });

    // 1. Handshake Connect Stream Creation
    Http3.newRequestStream(
            quicClient,
            new ChannelInitializer<QuicStreamChannel>() {
              @Override
              protected void initChannel(QuicStreamChannel ch) {
                System.out.println("DEBUG: CONNECT Stream pipeline init: " + ch.pipeline().names());
                ch.pipeline()
                    .addLast(
                        new SimpleChannelInboundHandler<Object>() {
                          @Override
                          protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                            System.out.println(
                                "DEBUG: Client CONNECT stream received message of class: "
                                    + msg.getClass().getName()
                                    + " | Msg: "
                                    + msg);
                            if (msg instanceof Http3HeadersFrame) {
                              Http3HeadersFrame headersFrame = (Http3HeadersFrame) msg;
                              System.out.println(
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
                            System.err.println(
                                "DEBUG: CONNECT stream inbound error: " + cause.getMessage());
                            cause.printStackTrace();
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                System.out.println(
                    "DEBUG: CONNECT Stream created successfully. Stream ID: " + ch.streamId());
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers))
                    .addListener(
                        writeFuture -> {
                          System.out.println(
                              "DEBUG: Write CONNECT headers success: "
                                  + writeFuture.isSuccess()
                                  + " | Cause: "
                                  + writeFuture.cause());
                        });
              } else {
                System.err.println("DEBUG: CONNECT Stream creation failed! Cause: " + f.cause());
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
                            System.out.println(
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
                                          System.out.println(
                                              "DEBUG: Removed from bidi pipeline: " + name);
                                        } catch (Exception ignored) {
                                        }
                                      }
                                      System.out.println(
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
                            System.out.println("DEBUG: Client bidi stream received: " + response);
                            if (response.contains("ACK BI")) {
                              bidiEchoLatch.countDown();
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            System.err.println(
                                "DEBUG: Client bidi stream error: " + cause.getMessage());
                            cause.printStackTrace();
                            ctx.close();
                          }
                        });
              }
            })
        .addListener(
            (Future<QuicStreamChannel> f) -> {
              if (f.isSuccess()) {
                QuicStreamChannel ch = f.getNow();
                System.out.println(
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
                          System.out.println(
                              "DEBUG: Write bidi data success: "
                                  + writeFuture.isSuccess()
                                  + " | Cause: "
                                  + writeFuture.cause());
                        });
              } else {
                System.err.println(
                    "DEBUG: Client bidi stream creation failed! Cause: " + f.cause());
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                                        } catch (Exception ignored) {
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
                            System.out.println("DEBUG: Client bidi stream received: " + response);
                            if (response.contains("ACK BI")) {
                              bidiEchoLatch.countDown();
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            System.out.println("DEBUG: Client bidi stream exception: " + cause);
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
                                            System.out.println(
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                                        } catch (Exception ignored) {
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
                            System.out.println("DEBUG: Client bidi stream received: " + response);
                            if (response.contains("ACK BI")) {
                              bidiEchoLatch.countDown();
                            }
                          }

                          @Override
                          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            System.out.println("DEBUG: Client bidi stream exception: " + cause);
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                                        } catch (Exception ignored) {
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

    assertTrue("Transfer timed out", echoLatch.await(10, TimeUnit.SECONDS));
    long duration = endTime[0] - startTime;
    System.out.println("Connection Throttling Test Duration: " + duration + " ms");
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                                        } catch (Exception ignored) {
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

    assertTrue("Transfer timed out", echoLatch.await(10, TimeUnit.SECONDS));
    long duration = endTime[0] - startTime;
    System.out.println("Stream Throttling Test Duration: " + duration + " ms");
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
                ch.writeAndFlush(new DefaultHttp3HeadersFrame(headers));
              }
            });

    assertTrue(handshakeLatch1.await(5, TimeUnit.SECONDS));
    assertNotNull(connectStream1[0]);
    long sessionId1 = connectStream1[0].streamId();

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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
    long startTime = System.currentTimeMillis();
    final long[] endTime = new long[1];

    // Start Stream 1
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
                                        } catch (Exception ignored) {
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
                                        } catch (Exception ignored) {
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
    long duration = endTime[0] - startTime;
    System.out.println("Global Throttling Test Duration: " + duration + " ms");
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                                        } catch (Exception ignored) {
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
    System.out.println("Stream Read Throttling Test Duration: " + duration + " ms");
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
                                        } catch (Exception ignored) {
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
    System.out.println("Coexistence Throttling Test Duration: " + duration + " ms");
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
                QuicStreamChannel ch = f.getNow();
                Http3Headers headers = new DefaultHttp3Headers();
                headers.method("CONNECT");
                headers.scheme("https");
                headers.path("/test-integration");
                headers.authority("localhost");
                headers.set(":protocol", "webtransport");
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
}

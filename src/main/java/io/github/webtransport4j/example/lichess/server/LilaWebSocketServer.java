package io.github.webtransport4j.example.lichess.server;

import io.github.webtransport4j.example.lichess.model.LilaGameRoom;
import io.github.webtransport4j.example.lichess.model.LilaLagTracker;
import io.github.webtransport4j.example.lichess.model.LilaMessage;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Ack;
import io.github.webtransport4j.example.lichess.model.LilaMessage.ChatSay;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Crowd;
import io.github.webtransport4j.example.lichess.model.LilaMessage.EvalGet;
import io.github.webtransport4j.example.lichess.model.LilaMessage.EvalHit;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Message;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Ping;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Pong;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Resync;
import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundMove;
import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundVersioned;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty WebSocket Server reproducing the Lichess {@code lila-ws} architecture.
 *
 * <p>All traffic (moves, ACKs, clocks, pings, crowd notifications, engine eval hits, and chat)
 * is multiplexed over a <b>single TCP stream</b> per client. Demonstrates the real-world
 * TCP Head-of-Line blocking and connection-drop penalties observed in production at Lichess.
 */
public class LilaWebSocketServer {

  private static final Logger log = LoggerFactory.getLogger(LilaWebSocketServer.class);

  private static final AttributeKey<ClientSession> SESSION_KEY = AttributeKey.valueOf("lila.session");

  private final String host;
  private final int requestedPort;
  private final Map<String, LilaGameRoom> rooms = new ConcurrentHashMap<>();
  private final LilaLagTracker lagTracker = new LilaLagTracker();

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel serverChannel;
  private SslContext sslContext;
  private int actualPort;

  /**
   * Session state associated with a WebSocket client channel.
   */
  public static final class ClientSession {
    public final String gameId;
    public final String playerId;
    public final String sri;
    public final long initialVersion;

    /**
     * Constructs a client session.
     *
     * @param gameId the game ID
     * @param playerId the player ID
     * @param sri the SRI session random identifier
     * @param initialVersion the starting socket version
     */
    public ClientSession(String gameId, String playerId, String sri, long initialVersion) {
      this.gameId = gameId;
      this.playerId = playerId;
      this.sri = sri;
      this.initialVersion = initialVersion;
    }
  }

  /**
   * Constructs a Lila WebSocket server.
   *
   * @param host host to bind
   * @param port port to bind (0 for dynamic port)
   */
  public LilaWebSocketServer(@NonNull String host, int port) {
    this.host = host;
    this.requestedPort = port;
  }

  /**
   * Starts the server.
   *
   * @throws Exception if startup fails
   */
  public void start() throws Exception {
    SelfSignedCertificate ssc = new SelfSignedCertificate();
    sslContext = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();

    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup(2);

    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(sslContext.newHandler(ch.alloc()));
            p.addLast(new HttpServerCodec());
            p.addLast(new HttpObjectAggregator(65536));
            p.addLast(new WebSocketServerProtocolHandler("", null, true, 65536, false, true, false) {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof FullHttpRequest) {
                  FullHttpRequest req = (FullHttpRequest) msg;
                  String uri = req.uri();
                  QueryStringDecoder qsd = new QueryStringDecoder(uri);

                  String gameId = "game-default";
                  String playerId = "p-white";
                  String sri = "sri-default";
                  long version = 0;

                  String[] parts = qsd.path().split("/");
                  if (parts.length >= 4 && "round".equals(parts[1])) {
                    gameId = parts[2];
                    playerId = parts[3];
                  }

                  if (qsd.parameters().containsKey("sri")) {
                    sri = qsd.parameters().get("sri").get(0);
                  }
                  if (qsd.parameters().containsKey("v")) {
                    try {
                      version = Long.parseLong(qsd.parameters().get("v").get(0));
                    } catch (NumberFormatException ignored) {
                      // default 0
                    }
                  }

                  ctx.channel().attr(SESSION_KEY).set(new ClientSession(gameId, playerId, sri, version));
                }
                super.channelRead(ctx, msg);
              }
            });
            p.addLast(new LilaWsFrameHandler());
          }
        });

    serverChannel = b.bind(host, requestedPort).sync().channel();
    actualPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
    log.info("Started Lila WebSocket Server (lila-ws replication) on {}:{}", host, actualPort);
  }

  /**
   * Stops the server and frees event loops.
   */
  public void stop() {
    if (serverChannel != null) {
      serverChannel.close();
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
  }

  public int getPort() {
    return actualPort;
  }

  public @NonNull LilaGameRoom getOrCreateRoom(@NonNull String gameId) {
    return rooms.computeIfAbsent(gameId, id -> new LilaGameRoom(id, "p-white", "p-black"));
  }

  public @NonNull LilaLagTracker getLagTracker() {
    return lagTracker;
  }

  /**
   * Inbound frame handler reproducing lila-ws FrameHandler & RoundClientActor logic.
   */
  private class LilaWsFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
        ClientSession session = ctx.channel().attr(SESSION_KEY).get();
        if (session != null) {
          LilaGameRoom room = getOrCreateRoom(session.gameId);
          // If the client reconnected with ?v=X, replay missed events or trigger resync
          if (session.initialVersion > 0) {
            List<RoundVersioned> missed = room.getHistoryFrom(session.initialVersion);
            if (missed == null) {
              // Gap is too large: send resync signal!
              ctx.writeAndFlush(new TextWebSocketFrame(Resync.INSTANCE.toJson()));
            } else {
              for (RoundVersioned ev : missed) {
                ctx.writeAndFlush(new TextWebSocketFrame(ev.toJson()));
              }
            }
          }
        }
      }
      super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
      if (!(frame instanceof TextWebSocketFrame)) {
        return;
      }

      String text = ((TextWebSocketFrame) frame).text();
      Message msg = LilaMessage.parse(text);
      if (msg == null) {
        return;
      }

      ClientSession session = ctx.channel().attr(SESSION_KEY).get();
      String gameId = session != null ? session.gameId : "game-default";
      String playerId = session != null ? session.playerId : "p-white";
      LilaGameRoom room = getOrCreateRoom(gameId);

      if (msg instanceof Ping) {
        Ping ping = (Ping) msg;
        if (ping.lagMillis() > 0 && session != null) {
          lagTracker.recordLag(session.sri, ping.lagMillis());
        }
        // Respond immediately with Pong
        ctx.writeAndFlush(new TextWebSocketFrame(new Pong(ping.timestamp()).toJson()));
      } else if (msg instanceof RoundMove) {
        RoundMove move = (RoundMove) msg;
        LilaGameRoom.MoveOutcome outcome = room.applyMove(playerId, move);
        if (outcome != null) {
          // Send ACK directly back to the player
          ctx.writeAndFlush(new TextWebSocketFrame(outcome.ack().toJson()));
          if (outcome.versioned() != null) {
            // Broadcast versioned state diff
            ctx.writeAndFlush(new TextWebSocketFrame(outcome.versioned().toJson()));
          }
        }
      } else if (msg instanceof EvalGet) {
        EvalGet eg = (EvalGet) msg;
        // Generate simulated 2KB - 4KB engine evaluation response
        EvalHit hit = new EvalHit(
            eg.fen(),
            24,
            35,
            "e2e4 c7c5 g1f3 d7d6 d2d4 c5d4 f3d4 g8f6 b1c3 a7a6 c1e3 e7e5 d4b3"
        );
        ctx.writeAndFlush(new TextWebSocketFrame(hit.toJson()));
      } else if (msg instanceof ChatSay) {
        ChatSay chat = (ChatSay) msg;
        ctx.writeAndFlush(new TextWebSocketFrame(chat.toJson()));
      }
    }
  }
}

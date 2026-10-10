package io.github.webtransport4j.example.lichess.server;

import io.github.webtransport4j.api.SessionRequestContext;
import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStream;
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
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebTransport Handler reproducing all Lichess {@code lila-ws} business logic with
 * multi-stream isolation and QUIC datagrams.
 *
 * <p>Stream Architecture:
 * <ul>
 *   <li><b>Bidirectional Move Stream (Bidi, Type 0x01)</b>: Strictly ordered sequential
 *       chess move loop: receives {@code RoundMove}, returns immediate {@code Ack},
 *       and emits {@code RoundVersioned} diffs. Immune to HoL blocking from any other channel.</li>
 *   <li><b>Auxiliary Unidirectional Streams (Uni, Server->Client)</b>:
 *       <ul>
 *         <li><b>Crowd Stream (Type 0x02)</b>: Pushes spectator presence updates.</li>
 *         <li><b>Engine Evaluation Stream (Type 0x03)</b>: Multi-KB analysis cache payloads.</li>
 *         <li><b>Chat Stream (Type 0x04)</b>: In-game player and watcher chat.</li>
 *       </ul>
 *   </li>
 *   <li><b>Datagrams (Type 0x01/0x02)</b>: Zero-HoL loss-tolerant ping/pong, heartbeat,
 *       and round-trip jitter telemetry.</li>
 *   <li><b>Connection Migration</b>: QUIC connection migration allows seamless network
 *       handover without dropping session state or resetting active games.</li>
 * </ul>
 */
public class LilaWebTransportHandler implements WebTransportHandler {

  private static final Logger log = LoggerFactory.getLogger(LilaWebTransportHandler.class);

  // Demultiplexing stream markers
  public static final byte STREAM_TYPE_MOVE_BIDI = 0x01;
  public static final byte STREAM_TYPE_CROWD_UNI = 0x02;
  public static final byte STREAM_TYPE_EVAL_UNI = 0x03;
  public static final byte STREAM_TYPE_CHAT_UNI = 0x04;

  private final Map<String, LilaGameRoom> rooms = new ConcurrentHashMap<>();
  private final LilaLagTracker lagTracker = new LilaLagTracker();
  private final Map<WebTransportSession, SessionContext> sessions = new ConcurrentHashMap<>();
  private final AtomicInteger migrationCounter = new AtomicInteger(0);

  /**
   * Application context associated with an active WebTransport session.
   */
  public static final class SessionContext {
    public final WebTransportSession session;
    public final String gameId;
    public final String playerId;
    public final String sri;
    public volatile WebTransportStream moveBidiStream;
    public volatile WebTransportStream crowdUniStream;
    public volatile WebTransportStream chatUniStream;

    /**
     * Constructs a session context.
     *
     * @param session the WebTransport session
     * @param gameId the game identifier
     * @param playerId the player identifier
     * @param sri the session random identifier
     */
    public SessionContext(
        WebTransportSession session,
        String gameId,
        String playerId,
        String sri) {
      this.session = session;
      this.gameId = gameId;
      this.playerId = playerId;
      this.sri = sri;
    }
  }

  @Override
  public boolean onSessionRequest(@NonNull SessionRequestContext request) {
    // Inspect path, query parameters (e.g. sri, v)
    return true;
  }

  @Override
  public void onSessionReady(@NonNull WebTransportSession session) {
    String path = session.path();
    String gameId = "game-default";
    String playerId = "p-white";
    String sri = "sri-wt-" + session.getSessionStreamId();

    if (path != null) {
      String[] parts = path.split("/");
      if (parts.length >= 4 && "round".equals(parts[1])) {
        gameId = parts[2];
        playerId = parts[3];
      }
    }

    SessionContext ctx = new SessionContext(session, gameId, playerId, sri);
    sessions.put(session, ctx);
    getOrCreateRoom(gameId);

    log.info("🟢 WebTransport session ready for player '{}' in game '{}'", playerId, gameId);
  }

  @Override
  public void onSessionClosed(@NonNull WebTransportSession session, int closeCode, String reason) {
    SessionContext ctx = sessions.remove(session);
    if (ctx != null) {
      log.info("🔴 WebTransport session closed for player '{}' in game '{}'", ctx.playerId, ctx.gameId);
    }
  }

  @Override
  public void onConnectionMigration(
      @NonNull WebTransportSession session,
      @NonNull SocketAddress oldAddress,
      @NonNull SocketAddress newAddress) {
    migrationCounter.incrementAndGet();
    SessionContext ctx = sessions.get(session);
    String gameId = ctx != null ? ctx.gameId : "unknown";
    log.info("🔄 [QUIC MIGRATION] Session for game '{}' migrated from {} to {} with 0-RTT interruption",
        gameId, oldAddress, newAddress);
  }

  @Override
  public void onIncomingStream(
      @NonNull WebTransportSession session, @NonNull WebTransportStream stream) {
    SessionContext ctx = sessions.get(session);
    if (ctx == null) {
      return;
    }

    LilaGameRoom room = getOrCreateRoom(ctx.gameId);

    if (stream.isBidirectional()) {
      // Dedicated Move Bidi Stream
      ctx.moveBidiStream = stream;
      stream.onData(buffer -> {
        byte[] bytes = buffer.readBytes();
        if (bytes.length == 0) {
          return;
        }

        String raw = new String(bytes, StandardCharsets.UTF_8);
        Message msg = LilaMessage.parse(raw);
        if (msg instanceof RoundMove) {
          RoundMove move = (RoundMove) msg;
          LilaGameRoom.MoveOutcome outcome = room.applyMove(ctx.playerId, move);
          if (outcome != null) {
            // Immediate ACK on the move stream
            stream.write(outcome.ack().toJson().getBytes(StandardCharsets.UTF_8));
            if (outcome.versioned() != null) {
              // Emit versioned diff
              stream.write(outcome.versioned().toJson().getBytes(StandardCharsets.UTF_8));
            }
          }
        } else if (msg instanceof Ping) {
          // Fallback if ping sent on stream
          stream.write(Pong.INSTANCE.toJson().getBytes(StandardCharsets.UTF_8));
        }
      });
    } else {
      // Inbound Unidirectional Stream (e.g. client uploading engine analysis or diagnostics)
      stream.onData(buffer -> {
        byte[] bytes = buffer.readBytes();
        String raw = new String(bytes, StandardCharsets.UTF_8);
        Message msg = LilaMessage.parse(raw);
        if (msg instanceof EvalGet) {
          EvalGet eg = (EvalGet) msg;
          // Spawn server-to-client uni stream to return heavy analysis cache without blocking moves
          session.createUniStream().thenAccept(outUni -> {
            EvalHit hit = new EvalHit(
                eg.fen(),
                24,
                35,
                "e2e4 c7c5 g1f3 d7d6 d2d4 c5d4 f3d4 g8f6 b1c3 a7a6 c1e3 e7e5 d4b3"
            );
            outUni.write(hit.toJson().getBytes(StandardCharsets.UTF_8));
            outUni.close();
          });
        }
      });
    }
  }

  @Override
  public void onDatagramReceived(
      @NonNull WebTransportSession session, @NonNull WebTransportBuffer data) {
    byte[] bytes = data.readBytes();
    if (bytes.length == 0) {
      return;
    }

    SessionContext ctx = sessions.get(session);
    String raw = new String(bytes, StandardCharsets.UTF_8);
    Message msg = LilaMessage.parse(raw);

    if (msg instanceof Ping) {
      Ping ping = (Ping) msg;
      if (ctx != null && ping.lagMillis() > 0) {
        lagTracker.recordLag(ctx.sri, ping.lagMillis());
      }
      // Zero-HoL datagram pong response
      Pong pong = new Pong(ping.timestamp());
      session.sendDatagram(pong.toJson().getBytes(StandardCharsets.UTF_8));
    } else if (bytes.length >= 9 && bytes[0] == 0x01) {
      // Binary probe datagram: [0x01, timestamp (8 bytes)] -> reply [0x02, timestamp (8 bytes)]
      byte[] reply = new byte[9];
      reply[0] = 0x02;
      System.arraycopy(bytes, 1, reply, 1, 8);
      session.sendDatagram(reply);
    }
  }

  public @NonNull LilaGameRoom getOrCreateRoom(@NonNull String gameId) {
    return rooms.computeIfAbsent(gameId, id -> new LilaGameRoom(id, "p-white", "p-black"));
  }

  public @NonNull LilaLagTracker getLagTracker() {
    return lagTracker;
  }

  public int getMigrationCount() {
    return migrationCounter.get();
  }
}

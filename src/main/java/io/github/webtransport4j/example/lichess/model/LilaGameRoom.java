package io.github.webtransport4j.example.lichess.model;

import io.github.webtransport4j.example.lichess.model.LilaMessage.Ack;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Crowd;
import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundMove;
import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundVersioned;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * State machine for an active Lichess game room replicating {@code RoundClientActor.scala}.
 *
 * <p>Tracks:
 * <ul>
 *   <li>White and Black players, active turn, clocks.</li>
 *   <li>Monotonic {@code SocketVersion} incremented on every game event.</li>
 *   <li>Circular version history for client reconnects and catch-up.</li>
 *   <li>Spectator presence and crowd notifications.</li>
 * </ul>
 */
public final class LilaGameRoom {

  private static final String STARTING_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";

  private final String gameId;
  private final String whitePlayerId;
  private final String blackPlayerId;
  private final AtomicLong socketVersion = new AtomicLong(1);
  private final LilaHistory history;

  private final Set<String> spectators = ConcurrentHashMap.newKeySet();
  private final List<String> movesPlayed = new ArrayList<>();
  private final Object gameLock = new Object();

  private int ply = 0;
  private int whiteClockCentis = 18000; // 3 minutes blitz default
  private int blackClockCentis = 18000;
  private String currentFen = STARTING_FEN;
  private boolean whiteTurn = true;

  /**
   * Constructs a game room with the given players.
   *
   * @param gameId the unique game identifier
   * @param whitePlayerId identifier of player with white pieces
   * @param blackPlayerId identifier of player with black pieces
   */
  public LilaGameRoom(
      @NonNull String gameId,
      @NonNull String whitePlayerId,
      @NonNull String blackPlayerId) {
    this.gameId = gameId;
    this.whitePlayerId = whitePlayerId;
    this.blackPlayerId = blackPlayerId;
    this.history = new LilaHistory(30);
  }

  public @NonNull String gameId() {
    return gameId;
  }

  public @NonNull String whitePlayerId() {
    return whitePlayerId;
  }

  public @NonNull String blackPlayerId() {
    return blackPlayerId;
  }

  public long currentVersion() {
    return socketVersion.get();
  }

  /**
   * Result of executing a move.
   */
  public static final class MoveOutcome {
    private final Ack ack;
    private final RoundVersioned versioned;

    public MoveOutcome(@NonNull Ack ack, @NonNull RoundVersioned versioned) {
      this.ack = ack;
      this.versioned = versioned;
    }

    public @NonNull Ack ack() {
      return ack;
    }

    public @NonNull RoundVersioned versioned() {
      return versioned;
    }
  }

  /**
   * Processes a player move, updating clocks, ply, and recording versioned state diff.
   *
   * @param playerId the identifier of the player attempting the move
   * @param move the move payload
   * @return the move outcome with ACK and broadcast event, or null if move rejected
   */
  public @Nullable MoveOutcome applyMove(@NonNull String playerId, @NonNull RoundMove move) {
    synchronized (gameLock) {
      boolean isWhite = playerId.equals(whitePlayerId);
      boolean isBlack = playerId.equals(blackPlayerId);

      if (!isWhite && !isBlack) {
        return null; // Spectator cannot move
      }

      // Check turn
      if ((isWhite && !whiteTurn) || (isBlack && whiteTurn)) {
        // Return ACK to clear client state even on out-of-turn rejection
        return new MoveOutcome(new Ack(move.ackId()), null);
      }

      ply++;
      whiteTurn = !whiteTurn;
      movesPlayed.add(move.uci());

      // Decrement clocks slightly simulating move time (e.g. 50 centiseconds = 0.5s)
      if (isWhite) {
        whiteClockCentis = Math.max(0, whiteClockCentis - Math.max(10, move.lagCentis()));
      } else {
        blackClockCentis = Math.max(0, blackClockCentis - Math.max(10, move.lagCentis()));
      }

      long newVersion = socketVersion.incrementAndGet();
      RoundVersioned versioned = new RoundVersioned(
          newVersion,
          "move",
          move.uci(),
          ply,
          whiteClockCentis,
          blackClockCentis,
          currentFen
      );

      history.add(versioned);
      Ack ack = new Ack(move.ackId());
      return new MoveOutcome(ack, versioned);
    }
  }

  /**
   * Connects a spectator to the room and returns updated crowd event.
   *
   * @param spectator username
   * @return crowd notification
   */
  public @NonNull Crowd addSpectator(@NonNull String spectator) {
    spectators.add(spectator);
    return getCrowd();
  }

  /**
   * Disconnects a spectator from the room and returns updated crowd event.
   *
   * @param spectator username
   * @return crowd notification
   */
  public @NonNull Crowd removeSpectator(@NonNull String spectator) {
    spectators.remove(spectator);
    return getCrowd();
  }

  /**
   * Generates current crowd presence message.
   *
   * @return crowd message
   */
  public @NonNull Crowd getCrowd() {
    List<String> list = new ArrayList<>(spectators);
    Collections.sort(list);
    return new Crowd(list.size() + 2, true, true, list);
  }

  /**
   * Replays missed events for a client reconnecting with version {@code sinceVersion}.
   *
   * @param sinceVersion client's last observed version
   * @return list of missed versioned events, or null if gap is too large (triggers resync)
   */
  public @Nullable List<RoundVersioned> getHistoryFrom(long sinceVersion) {
    return history.getFrom(sinceVersion);
  }

  /**
   * Generates a full game state JSON payload.
   *
   * @return full game state JSON
   */
  public @NonNull String getFullState() {
    synchronized (gameLock) {
      return "{\"t\":\"full\",\"v\":" + socketVersion.get()
          + ",\"d\":{\"id\":\"" + gameId + "\",\"ply\":" + ply
          + ",\"wc\":" + whiteClockCentis + ",\"bc\":" + blackClockCentis
          + ",\"fen\":\"" + currentFen + "\"}}";
    }
  }
}

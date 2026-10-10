package io.github.webtransport4j.example.lichess.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Message definitions and lightweight codec reproducing the Lichess (lila-ws) protocol.
 *
 * <p>Supports:
 * <ul>
 *   <li>{@code Ping} / {@code Pong}: Heartbeat and latency tracking.</li>
 *   <li>{@code RoundMove}: UCI move (e.g. "e2e4"), ackId, blur flag, client lag.</li>
 *   <li>{@code Ack}: Acknowledgment for move/berserk.</li>
 *   <li>{@code RoundVersioned}: Versioned state change (move, clock, berserk, flag).</li>
 *   <li>{@code Crowd}: Spectator presence notifications.</li>
 *   <li>{@code EvalGet} / {@code EvalHit}: Engine evaluation requests and multi-KB cache responses.</li>
 *   <li>{@code ChatSay}: In-game chat.</li>
 *   <li>{@code Resync}: Tells the client to reload state when history gap is too large.</li>
 * </ul>
 */
public final class LilaMessage {

  private LilaMessage() {}

  /** Base interface for Lichess messages. */
  public interface Message {
    /** Returns message type identifier matching Lichess protocol. */
    @NonNull String type();

    /** Serializes the message to JSON matching lila-ws format. */
    @NonNull String toJson();
  }

  // ==========================================
  // Client -> Server Messages (ClientOut)
  // ==========================================

  /** Ping message sent from client. */
  public static final class Ping implements Message {
    private final long timestamp;
    private final int lagMillis;

    /**
     * Constructs a Ping message.
     *
     * @param timestamp client timestamp in milliseconds
     * @param lagMillis client observed lag in milliseconds
     */
    public Ping(long timestamp, int lagMillis) {
      this.timestamp = timestamp;
      this.lagMillis = lagMillis;
    }

    public long timestamp() {
      return timestamp;
    }

    public int lagMillis() {
      return lagMillis;
    }

    @Override
    public @NonNull String type() {
      return "p";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"p\",\"ts\":" + timestamp + ",\"l\":" + lagMillis + "}";
    }
  }

  /** Chess move message sent from player client. */
  public static final class RoundMove implements Message {
    private final String uci;
    private final int ackId;
    private final boolean blur;
    private final int lagCentis;

    /**
     * Constructs a RoundMove message.
     *
     * @param uci the UCI move string
     * @param ackId sequence acknowledgment ID
     * @param blur whether window lost focus
     * @param lagCentis client move lag in centiseconds
     */
    public RoundMove(@NonNull String uci, int ackId, boolean blur, int lagCentis) {
      this.uci = uci;
      this.ackId = ackId;
      this.blur = blur;
      this.lagCentis = lagCentis;
    }

    public @NonNull String uci() {
      return uci;
    }

    public int ackId() {
      return ackId;
    }

    public boolean blur() {
      return blur;
    }

    public int lagCentis() {
      return lagCentis;
    }

    @Override
    public @NonNull String type() {
      return "move";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"move\",\"d\":{\"u\":\"" + uci + "\",\"a\":" + ackId
          + ",\"b\":" + (blur ? 1 : 0) + ",\"l\":" + lagCentis + "}}";
    }
  }

  /** Player going berserk in tournament. */
  public static final class RoundBerserk implements Message {
    private final int ackId;

    /**
     * Constructs a RoundBerserk message.
     *
     * @param ackId sequence acknowledgment ID
     */
    public RoundBerserk(int ackId) {
      this.ackId = ackId;
    }

    public int ackId() {
      return ackId;
    }

    @Override
    public @NonNull String type() {
      return "berserk";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"berserk\",\"d\":{\"a\":" + ackId + "}}";
    }
  }

  /** Chat message from player or watcher. */
  public static final class ChatSay implements Message {
    private final String user;
    private final String text;

    /**
     * Constructs a ChatSay message.
     *
     * @param user username
     * @param text message text
     */
    public ChatSay(@NonNull String user, @NonNull String text) {
      this.user = user;
      this.text = text;
    }

    public @NonNull String user() {
      return user;
    }

    public @NonNull String text() {
      return text;
    }

    @Override
    public @NonNull String type() {
      return "talk";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"talk\",\"d\":\"" + escape(text) + "\",\"u\":\"" + escape(user) + "\"}";
    }
  }

  /** Request for chess engine evaluation cache. */
  public static final class EvalGet implements Message {
    private final String fen;

    /**
     * Constructs an EvalGet message.
     *
     * @param fen board FEN position
     */
    public EvalGet(@NonNull String fen) {
      this.fen = fen;
    }

    public @NonNull String fen() {
      return fen;
    }

    @Override
    public @NonNull String type() {
      return "evalGet";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"evalGet\",\"d\":{\"fen\":\"" + escape(fen) + "\"}}";
    }
  }

  // ==========================================
  // Server -> Client Messages (ClientIn)
  // ==========================================

  /** Single-character Pong reply matching lila-ws format ("0"). */
  public static final class Pong implements Message {
    public static final Pong INSTANCE = new Pong(0);
    private final long clientTimestamp;

    /**
     * Constructs a Pong message.
     *
     * @param clientTimestamp timestamp echoed back to client
     */
    public Pong(long clientTimestamp) {
      this.clientTimestamp = clientTimestamp;
    }

    public long clientTimestamp() {
      return clientTimestamp;
    }

    @Override
    public @NonNull String type() {
      return "0";
    }

    @Override
    public @NonNull String toJson() {
      return clientTimestamp > 0 ? "{\"t\":\"0\",\"ts\":" + clientTimestamp + "}" : "0";
    }
  }

  /** Acknowledgment for move or berserk. */
  public static final class Ack implements Message {
    private final int ackId;

    /**
     * Constructs an Ack message.
     *
     * @param ackId sequence acknowledgment ID
     */
    public Ack(int ackId) {
      this.ackId = ackId;
    }

    public int ackId() {
      return ackId;
    }

    @Override
    public @NonNull String type() {
      return "ack";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"ack\",\"d\":" + ackId + "}";
    }
  }

  /**
   * Versioned game state update with monotonically increasing socket version.
   * Directly reproduces {@code ClientIn.RoundVersioned} in lila-ws.
   */
  public static final class RoundVersioned implements Message {
    private final long version;
    private final String eventType;
    private final String uci;
    private final int ply;
    private final int whiteClockCentis;
    private final int blackClockCentis;
    private final String fen;

    /**
     * Constructs a RoundVersioned message.
     *
     * @param version the socket version
     * @param eventType the event type
     * @param uci the UCI move notation
     * @param ply the move count
     * @param whiteClockCentis white's remaining clock time in centiseconds
     * @param blackClockCentis black's remaining clock time in centiseconds
     * @param fen current board FEN
     */
    public RoundVersioned(
        long version,
        @NonNull String eventType,
        @NonNull String uci,
        int ply,
        int whiteClockCentis,
        int blackClockCentis,
        @NonNull String fen) {
      this.version = version;
      this.eventType = eventType;
      this.uci = uci;
      this.ply = ply;
      this.whiteClockCentis = whiteClockCentis;
      this.blackClockCentis = blackClockCentis;
      this.fen = fen;
    }

    public long version() {
      return version;
    }

    public @NonNull String eventType() {
      return eventType;
    }

    public @NonNull String uci() {
      return uci;
    }

    public int ply() {
      return ply;
    }

    public int whiteClockCentis() {
      return whiteClockCentis;
    }

    public int blackClockCentis() {
      return blackClockCentis;
    }

    public @NonNull String fen() {
      return fen;
    }

    @Override
    public @NonNull String type() {
      return eventType;
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"" + eventType + "\",\"v\":" + version
          + ",\"d\":{\"u\":\"" + uci + "\",\"ply\":" + ply
          + ",\"wc\":" + whiteClockCentis + ",\"bc\":" + blackClockCentis
          + ",\"fen\":\"" + escape(fen) + "\"}}";
    }
  }

  /** Spectator crowd update notification. */
  public static final class Crowd implements Message {
    private final int members;
    private final boolean whitePresent;
    private final boolean blackPresent;
    private final List<String> watchers;

    /**
     * Constructs a Crowd update message.
     *
     * @param members total member count
     * @param whitePresent true if white player is connected
     * @param blackPresent true if black player is connected
     * @param watchers list of active spectator usernames
     */
    public Crowd(int members, boolean whitePresent, boolean blackPresent, @NonNull List<String> watchers) {
      this.members = members;
      this.whitePresent = whitePresent;
      this.blackPresent = blackPresent;
      this.watchers = Collections.unmodifiableList(new ArrayList<>(watchers));
    }

    public int members() {
      return members;
    }

    public boolean whitePresent() {
      return whitePresent;
    }

    public boolean blackPresent() {
      return blackPresent;
    }

    public @NonNull List<String> watchers() {
      return watchers;
    }

    @Override
    public @NonNull String type() {
      return "crowd";
    }

    @Override
    public @NonNull String toJson() {
      StringBuilder sb = new StringBuilder();
      sb.append("{\"t\":\"crowd\",\"d\":{\"members\":").append(members)
          .append(",\"w\":").append(whitePresent)
          .append(",\"b\":").append(blackPresent)
          .append(",\"watchers\":[");
      for (int i = 0; i < watchers.size(); i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append("\"").append(escape(watchers.get(i))).append("\"");
      }
      sb.append("]}}");
      return sb.toString();
    }
  }

  /** Heavy evaluation cache hit (1KB - 4KB). */
  public static final class EvalHit implements Message {
    private final String fen;
    private final int depth;
    private final int scoreCentipawns;
    private final String principalVariation;

    /**
     * Constructs an EvalHit message.
     *
     * @param fen board FEN
     * @param depth search depth
     * @param scoreCentipawns score in centipawns
     * @param principalVariation principal variation line
     */
    public EvalHit(
        @NonNull String fen,
        int depth,
        int scoreCentipawns,
        @NonNull String principalVariation) {
      this.fen = fen;
      this.depth = depth;
      this.scoreCentipawns = scoreCentipawns;
      this.principalVariation = principalVariation;
    }

    public @NonNull String fen() {
      return fen;
    }

    public int depth() {
      return depth;
    }

    public int scoreCentipawns() {
      return scoreCentipawns;
    }

    public @NonNull String principalVariation() {
      return principalVariation;
    }

    @Override
    public @NonNull String type() {
      return "evalHit";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"evalHit\",\"d\":{\"fen\":\"" + escape(fen) + "\",\"depth\":" + depth
          + ",\"score\":" + scoreCentipawns + ",\"pvs\":[\"" + escape(principalVariation) + "\"]"
          + ",\"knodes\":14820,\"time\":1240}}";
    }
  }

  /** Resync signal sent when client version is too far behind history buffer. */
  public static final class Resync implements Message {
    public static final Resync INSTANCE = new Resync();

    @Override
    public @NonNull String type() {
      return "resync";
    }

    @Override
    public @NonNull String toJson() {
      return "{\"t\":\"resync\"}";
    }
  }

  // ==========================================
  // Parser
  // ==========================================

  /**
   * Fast, lightweight parsing of incoming Lichess JSON frames.
   *
   * @param raw the raw text frame or datagram string
   * @return parsed message or null if unrecognized
   */
  public static @Nullable Message parse(@NonNull String raw) {
    String trimmed = raw.trim();
    if ("p".equals(trimmed) || "null".equals(trimmed) || "{\"t\":\"p\"}".equals(trimmed)) {
      return new Ping(System.currentTimeMillis(), 0);
    }
    if ("0".equals(trimmed)) {
      return Pong.INSTANCE;
    }

    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
      return null;
    }

    String type = extractField(trimmed, "t");
    if (type == null) {
      return null;
    }

    switch (type) {
      case "p": {
        long ts = extractLongField(trimmed, "ts", System.currentTimeMillis());
        int lag = extractIntField(trimmed, "l", 0);
        return new Ping(ts, lag);
      }
      case "move": {
        String uci = extractNestedField(trimmed, "d", "u");
        int ackId = extractNestedIntField(trimmed, "d", "a", 0);
        int blurInt = extractNestedIntField(trimmed, "d", "b", 0);
        int lag = extractNestedIntField(trimmed, "d", "l", 0);
        if (uci != null) {
          return new RoundMove(uci, ackId, blurInt == 1, lag);
        }
        return null;
      }
      case "berserk": {
        int ackId = extractNestedIntField(trimmed, "d", "a", 0);
        return new RoundBerserk(ackId);
      }
      case "talk": {
        String text = extractField(trimmed, "d");
        String user = extractField(trimmed, "u");
        return new ChatSay(user != null ? user : "anon", text != null ? text : "");
      }
      case "evalGet": {
        String fen = extractNestedField(trimmed, "d", "fen");
        return fen != null ? new EvalGet(fen) : null;
      }
      case "ack": {
        int ackId = extractIntField(trimmed, "d", 0);
        return new Ack(ackId);
      }
      case "resync": {
        return Resync.INSTANCE;
      }
      default:
        return null;
    }
  }

  // Simple string extractors avoiding heavy dependencies
  private static @Nullable String extractField(String json, String field) {
    String pattern = "\"" + field + "\":";
    int idx = json.indexOf(pattern);
    if (idx < 0) {
      return null;
    }
    int start = idx + pattern.length();
    while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
      start++;
    }
    if (start >= json.length()) {
      return null;
    }
    if (json.charAt(start) == '"') {
      int end = json.indexOf('"', start + 1);
      return end > start ? json.substring(start + 1, end) : null;
    }
    int end = start;
    while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') {
      end++;
    }
    return json.substring(start, end).trim();
  }

  private static int extractIntField(String json, String field, int defaultValue) {
    String val = extractField(json, field);
    if (val != null) {
      try {
        return Integer.parseInt(val);
      } catch (NumberFormatException ignored) {
        // Return default
      }
    }
    return defaultValue;
  }

  private static long extractLongField(String json, String field, long defaultValue) {
    String val = extractField(json, field);
    if (val != null) {
      try {
        return Long.parseLong(val);
      } catch (NumberFormatException ignored) {
        // Return default
      }
    }
    return defaultValue;
  }

  private static @Nullable String extractNestedField(String json, String parent, String child) {
    String parentPattern = "\"" + parent + "\":";
    int parentIdx = json.indexOf(parentPattern);
    if (parentIdx < 0) {
      return null;
    }
    int subStart = json.indexOf('{', parentIdx + parentPattern.length());
    if (subStart < 0) {
      return null;
    }
    int subEnd = json.indexOf('}', subStart);
    if (subEnd < 0) {
      return null;
    }
    String sub = json.substring(subStart, subEnd + 1);
    return extractField(sub, child);
  }

  private static int extractNestedIntField(String json, String parent, String child, int defaultValue) {
    String val = extractNestedField(json, parent, child);
    if (val != null) {
      try {
        return Integer.parseInt(val);
      } catch (NumberFormatException ignored) {
        // Return default
      }
    }
    return defaultValue;
  }

  private static String escape(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}

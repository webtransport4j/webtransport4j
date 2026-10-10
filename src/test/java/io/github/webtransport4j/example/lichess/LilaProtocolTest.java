package io.github.webtransport4j.example.lichess;

import io.github.webtransport4j.example.lichess.model.LilaGameRoom;
import io.github.webtransport4j.example.lichess.model.LilaHistory;
import io.github.webtransport4j.example.lichess.model.LilaLagTracker;
import io.github.webtransport4j.example.lichess.model.LilaMessage;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Ack;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Crowd;
import io.github.webtransport4j.example.lichess.model.LilaMessage.EvalGet;
import io.github.webtransport4j.example.lichess.model.LilaMessage.EvalHit;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Message;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Ping;
import io.github.webtransport4j.example.lichess.model.LilaMessage.Pong;
import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundMove;
import io.github.webtransport4j.example.lichess.model.LilaMessage.RoundVersioned;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests verifying reproduced Lichess protocol and business logic.
 */
public class LilaProtocolTest {

  @Test
  public void testMessageParsingAndSerialization() {
    // Ping
    Ping ping = new Ping(123456789L, 45);
    Message parsedPing = LilaMessage.parse(ping.toJson());
    Assert.assertTrue(parsedPing instanceof Ping);
    Assert.assertEquals(123456789L, ((Ping) parsedPing).timestamp());
    Assert.assertEquals(45, ((Ping) parsedPing).lagMillis());

    // Single-char ping/pong
    Assert.assertTrue(LilaMessage.parse("p") instanceof Ping);
    Assert.assertTrue(LilaMessage.parse("0") instanceof Pong);

    // Move
    RoundMove move = new RoundMove("e2e4", 42, false, 15);
    Message parsedMove = LilaMessage.parse(move.toJson());
    Assert.assertTrue(parsedMove instanceof RoundMove);
    RoundMove pm = (RoundMove) parsedMove;
    Assert.assertEquals("e2e4", pm.uci());
    Assert.assertEquals(42, pm.ackId());
    Assert.assertFalse(pm.blur());
    Assert.assertEquals(15, pm.lagCentis());

    // Eval
    EvalGet eg = new EvalGet("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1");
    Message parsedEg = LilaMessage.parse(eg.toJson());
    Assert.assertTrue(parsedEg instanceof EvalGet);
    Assert.assertEquals(eg.fen(), ((EvalGet) parsedEg).fen());
  }

  @Test
  public void testGameRoomMoveExecutionAndVersionIncrements() {
    LilaGameRoom room = new LilaGameRoom("g-test", "alice", "bob");
    Assert.assertEquals(1L, room.currentVersion());

    // White's turn: alice moves e2e4
    RoundMove m1 = new RoundMove("e2e4", 101, false, 20);
    LilaGameRoom.MoveOutcome outcome1 = room.applyMove("alice", m1);
    Assert.assertNotNull(outcome1);
    Assert.assertEquals(101, outcome1.ack().ackId());
    Assert.assertEquals(2L, outcome1.versioned().version());
    Assert.assertEquals("e2e4", outcome1.versioned().uci());
    Assert.assertEquals(1, outcome1.versioned().ply());

    // Out of turn: alice attempts second move -> rejected
    RoundMove m2Out = new RoundMove("d2d4", 102, false, 10);
    LilaGameRoom.MoveOutcome rejected = room.applyMove("alice", m2Out);
    Assert.assertNotNull(rejected);
    Assert.assertNull(rejected.versioned()); // No version increment or state diff

    // Black's turn: bob moves e7e5
    RoundMove m2 = new RoundMove("e7e5", 201, false, 25);
    LilaGameRoom.MoveOutcome outcome2 = room.applyMove("bob", m2);
    Assert.assertNotNull(outcome2);
    Assert.assertEquals(201, outcome2.ack().ackId());
    Assert.assertEquals(3L, outcome2.versioned().version());
    Assert.assertEquals("e7e5", outcome2.versioned().uci());
    Assert.assertEquals(2, outcome2.versioned().ply());
  }

  @Test
  public void testHistoryCatchupAndResyncBoundary() {
    LilaHistory history = new LilaHistory(5);

    for (int i = 1; i <= 5; i++) {
      history.add(new RoundVersioned(i, "move", "m" + i, i, 18000, 18000, "fen"));
    }
    Assert.assertEquals(5, history.size());

    // Client at version 2 catches up to 5: gets versions 3, 4, 5
    List<RoundVersioned> caughtUp = history.getFrom(2);
    Assert.assertNotNull(caughtUp);
    Assert.assertEquals(3, caughtUp.size());
    Assert.assertEquals(3L, caughtUp.get(0).version());
    Assert.assertEquals(5L, caughtUp.get(2).version());

    // Client already at latest version 5: gets empty list (up to date)
    List<RoundVersioned> upToDate = history.getFrom(5);
    Assert.assertNotNull(upToDate);
    Assert.assertTrue(upToDate.isEmpty());

    // Add 6 more events, pushing oldest version to 7
    for (int i = 6; i <= 11; i++) {
      history.add(new RoundVersioned(i, "move", "m" + i, i, 18000, 18000, "fen"));
    }

    // Client at version 2: gap is too large! Returns null (triggering full resync)
    List<RoundVersioned> tooOld = history.getFrom(2);
    Assert.assertNull(tooOld);
  }

  @Test
  public void testCrowdPresenceManagement() {
    LilaGameRoom room = new LilaGameRoom("g-crowd", "white", "black");
    Crowd c1 = room.addSpectator("spectator1");
    Assert.assertEquals(3, c1.members()); // 2 players + 1 spectator
    Assert.assertTrue(c1.watchers().contains("spectator1"));

    Crowd c2 = room.addSpectator("spectator2");
    Assert.assertEquals(4, c2.members());

    Crowd c3 = room.removeSpectator("spectator1");
    Assert.assertEquals(3, c3.members());
    Assert.assertFalse(c3.watchers().contains("spectator1"));
  }

  @Test
  public void testLagTrackerSmoothing() {
    LilaLagTracker tracker = new LilaLagTracker();
    int initial = tracker.recordLag("user1", 100);
    Assert.assertEquals(100, initial);

    // Apply EMA update: (100 * 0.9) + (50 * 0.1) = 90 + 5 = 95
    int smoothed = tracker.recordLag("user1", 50);
    Assert.assertEquals(95, smoothed);
  }
}

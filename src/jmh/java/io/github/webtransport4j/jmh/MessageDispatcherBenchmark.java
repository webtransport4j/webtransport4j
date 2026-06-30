package io.github.webtransport4j.jmh;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.DefaultMessageDispatcher;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportDatagramFrame;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** JMH benchmark for MessageDispatcher performance. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MessageDispatcherBenchmark {

  private DefaultMessageDispatcher dispatcher;
  private EmbeddedChannel channel;
  private WebTransportDatagramFrame datagramFrame;

  /** Sets up test fixtures. */
  @Setup
  public void setup() {
    dispatcher = new DefaultMessageDispatcher();
    channel = new EmbeddedChannel(dispatcher);

    // Fast native Netty attributes instead of slow Mockito mocks for Channel/Attribute
    WebTransportSessionManager mockMgr = Mockito.mock(WebTransportSessionManager.class);
    WebTransportSession session = Mockito.mock(WebTransportSession.class);
    Mockito.when(session.path()).thenReturn("/test");
    Mockito.when(mockMgr.get(1L)).thenReturn(session);

    channel.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mockMgr);

    WebTransportServer server =
        new WebTransportServer(
            new WebTransportHandler() {
              @Override
              public void onDatagramReceived(WebTransportSession session, WebTransportBuffer data) {
                // Do nothing to isolate dispatcher performance
              }
            });
    channel.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);

    ByteBuf buf = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4, 5});
    datagramFrame = new WebTransportDatagramFrame(1L, buf);
  }

  /** Test Dispatch Datagram Frame. */
  @Benchmark
  public void testDispatchDatagramFrame() {
    datagramFrame.retain();
    channel.writeInbound(datagramFrame);
    channel.runPendingTasks();
  }
}

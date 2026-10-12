package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportBuffer;
import io.github.webtransport4j.api.WebTransportHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Test;

/** Unit tests for slow handler detection and executor offload dispatching. */
public class DispatcherBlockingSafetyTest {

  private EmbeddedChannel channel(
      WebTransportHandler handler, MessageDispatcher dispatcher) {
    EmbeddedChannel channel = new EmbeddedChannel(dispatcher);
    NettyWebTransportSession session = mock(NettyWebTransportSession.class);
    when(session.path()).thenReturn("/test");
    when(session.isOpen()).thenReturn(true);
    WebTransportSessionManager manager = mock(WebTransportSessionManager.class);
    when(manager.get(0L)).thenReturn(session);
    WebTransportServer server = mock(WebTransportServer.class);
    when(server.getHandler("/test")).thenReturn(handler);
    channel.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);
    channel.attr(WebTransportAttributeKeys.SERVER_KEY).set(server);
    return channel;
  }

  private ByteBuf datagram(int value) {
    return Unpooled.buffer().writeByte(0).writeByte(value);
  }

  private static final class WarnCaptureAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    WarnCaptureAppender(String name) {
      super(name, null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }
  }

  private WarnCaptureAppender attachWarnAppender(String name) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();
    WarnCaptureAppender appender = new WarnCaptureAppender(name);
    appender.start();
    configuration.addAppender(appender);
    configuration.getRootLogger().addAppender(appender, Level.WARN, null);
    context.updateLoggers();
    return appender;
  }

  private void detachWarnAppender(WarnCaptureAppender appender) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();
    configuration.getRootLogger().removeAppender(appender.getName());
    configuration.getAppenders().remove(appender.getName());
    appender.stop();
    context.updateLoggers();
  }

  @Test
  public void slowHandlerCallbackWarnsOncePerChannel() throws Exception {
    WarnCaptureAppender appender = attachWarnAppender("slowHandlerWarnCapture");
    WebTransportConfig.setProperty(
        DefaultMessageDispatcher.SLOW_HANDLER_WARN_MS_KEY, "1");
    try {
      EmbeddedChannel ch =
          channel(
              new WebTransportHandler() {
                @Override
                public void onDatagramReceived(
                    io.github.webtransport4j.api.WebTransportSession session,
                    WebTransportBuffer buffer) {
                  try {
                    Thread.sleep(50);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                }
              },
              new DefaultMessageDispatcher());
      ByteBuf first = datagram(1);
      ByteBuf second = datagram(2);
      try {
        ch.writeInbound(first);
        ch.writeInbound(second);
        List<LogEvent> warns = appender.events;
        assertEquals(1, warns.size());
        String message = warns.get(0).getMessage().getFormattedMessage();
        assertTrue(message.contains("onDatagramReceived"));
        assertTrue(message.contains("Slow WebTransport handler"));
        assertEquals(
            Boolean.TRUE,
            ch.attr(WebTransportAttributeKeys.SLOW_HANDLER_WARNED).get());
        assertEquals(0, first.refCnt());
        assertEquals(0, second.refCnt());
      } finally {
        ch.finishAndReleaseAll();
      }
    } finally {
      WebTransportConfig.removeProperty(DefaultMessageDispatcher.SLOW_HANDLER_WARN_MS_KEY);
      detachWarnAppender(appender);
    }
  }

  @Test
  public void offloadDispatchPreservesSequentialDatagramOrder() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ExecutorOffloadDispatcher dispatcher = new ExecutorOffloadDispatcher(executor, true);
    List<Integer> received = new CopyOnWriteArrayList<>();
    CountDownLatch done = new CountDownLatch(3);
    EmbeddedChannel ch =
        channel(
            new WebTransportHandler() {
              @Override
              public void onDatagramReceived(
                  io.github.webtransport4j.api.WebTransportSession session,
                  WebTransportBuffer buffer) {
                received.add((int) buffer.getByte(0));
                done.countDown();
              }
            },
            dispatcher);
    ByteBuf first = datagram(1);
    ByteBuf second = datagram(2);
    ByteBuf third = datagram(3);
    try {
      ch.writeInbound(first);
      ch.writeInbound(second);
      ch.writeInbound(third);
      assertTrue(done.await(10, TimeUnit.SECONDS));
      assertEquals(Arrays.asList(1, 2, 3), received);
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      assertEquals(0, first.refCnt());
      assertEquals(0, second.refCnt());
      assertEquals(0, third.refCnt());
    } finally {
      ch.finishAndReleaseAll();
      executor.shutdownNow();
    }
  }

  @Test
  public void offloadDispatchRejectionReleasesPayload() {
    ExecutorService rejecting = mock(ExecutorService.class);
    doThrow(new RejectedExecutionException("pool full"))
        .when(rejecting)
        .execute(any(Runnable.class));
    EmbeddedChannel ch =
        channel(
            new WebTransportHandler() {},
            new ExecutorOffloadDispatcher(rejecting));
    ByteBuf input = datagram(9);
    try {
      ch.writeInbound(input);
      assertEquals(0, input.refCnt());
    } finally {
      ch.finishAndReleaseAll();
    }
  }
}

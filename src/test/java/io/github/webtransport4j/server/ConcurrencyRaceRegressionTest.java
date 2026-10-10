package io.github.webtransport4j.server;

import static io.github.webtransport4j.concurrency.ConcurrencySupport.await;
import static io.github.webtransport4j.concurrency.ConcurrencySupport.awaitCompletionOrBlockedBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportFlowPublisher;
import io.github.webtransport4j.concurrency.ConcurrencySupport;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.AttributeKey;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/** Regression invariants for the Java races originally reproduced from TLA+ counterexamples. */
public class ConcurrencyRaceRegressionTest {
  @Test(timeout = 15000)
  public void emitThatPassedGateMustNotLeakAfterCompletion() throws Exception {
    WebTransportFlowPublisher<Item> publisher = new WebTransportFlowPublisher<>();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    ConcurrentLinkedQueue<Item> controlled =
        new ConcurrentLinkedQueue<Item>() {
          public boolean offer(Item item) {
            entered.countDown();
            await(resume);
            return super.offer(item);
          }
        };
    ConcurrencySupport.replace(publisher, "queue", controlled);
    AtomicInteger terminals = new AtomicInteger();
    publisher.subscribe(
        new Subscriber<Item>() {
          public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
          }

          public void onNext(Item item) {
            item.close();
          }

          public void onError(Throwable error) {
            terminals.incrementAndGet();
          }

          public void onComplete() {
            terminals.incrementAndGet();
          }
        });
    Item item = new Item();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> emission = executor.submit(() -> publisher.emitNext(item));
      await(entered);
      publisher.emitComplete();
      assertEquals(1, terminals.get());
      resume.countDown();
      emission.get(5, TimeUnit.SECONDS);
      assertEquals("Post-terminal emitter stranded its closeable item", 1, item.closed.get());
      assertTrue(controlled.isEmpty());
    } finally {
      resume.countDown();
      executor.shutdownNow();
      Item leftover;
      while ((leftover = controlled.poll()) != null) {
        leftover.close();
      }
    }
  }

  @Test(timeout = 15000)
  public void delayedTlsInstallationMustNotRepublishAfterClose() throws Exception {
    WebTransportServer server = WebTransportServer.builder().port(0).build();
    Method install =
        WebTransportServer.class.getDeclaredMethod(
            "installReloadedSslContext", QuicSslContext.class);
    install.setAccessible(true);
    CountDownLatch built = new CountDownLatch(1);
    CountDownLatch installNow = new CountDownLatch(1);
    QuicSslContext context = mock(QuicSslContext.class);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> reload =
          executor.submit(
              () -> {
                built.countDown();
                await(installNow);
                try {
                  install.invoke(server, context);
                } catch (Exception e) {
                  throw new AssertionError(e);
                }
              });
      await(built);
      server.close();
      installNow.countDown();
      reload.get(5, TimeUnit.SECONDS);
      assertNull("Closed server accepted a delayed reload callback", server.getActiveSslContext());
    } finally {
      installNow.countDown();
      executor.shutdownNow();
      server.close();
    }
  }

  @Test(timeout = 15000)
  @SuppressWarnings("unchecked")
  public void concurrentStreamCreationMustNotOverspendPeerCredit() throws Exception {
    QuicChannel parent = mock(QuicChannel.class);
    QuicStreamChannel connect = mock(QuicStreamChannel.class);
    when(connect.parent()).thenReturn(parent);
    when(connect.isOpen()).thenReturn(true);
    io.netty.channel.DefaultEventLoop loop = new io.netty.channel.DefaultEventLoop();
    when(parent.eventLoop()).thenReturn(loop);
    when(parent.createStream(any(QuicStreamType.class), any(ChannelHandler.class)))
        .thenAnswer(
            call ->
                ImmediateEventExecutor.INSTANCE.newFailedFuture(
                    new Exception("controlled failure")));
    CountDownLatch firstRead = new CountDownLatch(1);
    CountDownLatch resumeRead = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    AtomicBoolean firstReader = new AtomicBoolean(true);
    AtomicReference<Thread> owner = new AtomicReference<>();
    AtomicReference<Thread> contender = new AtomicReference<>();
    DefaultWebTransportSession session =
        new DefaultWebTransportSession(0, connect, "/test", 1, 1, 100, 1, 1, 100, true, true) {
          public long getServerInitiatedStreamsBidi() {
            long before = super.getServerInitiatedStreamsBidi();
            if (firstReader.compareAndSet(true, false)) {
              owner.set(Thread.currentThread());
              firstRead.countDown();
              await(resumeRead);
            }
            return before;
          }
        };
    WebTransportSessionManager manager = mock(WebTransportSessionManager.class);
    when(manager.get(0)).thenReturn(session);
    DefaultAttributeMap attributes = new DefaultAttributeMap();
    attributes.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(manager);
    when(parent.attr(any(AttributeKey.class)))
        .thenAnswer(call -> attributes.attr(call.getArgument(0)));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> first =
          executor.submit(
              () ->
                  WebTransportUtils.createBiStream(
                      connect, false, DefaultWebTransportSession.DEFAULT_BI_INITIALIZER));
      await(firstRead);
      final Future<?> second =
          executor.submit(
              () -> {
                contender.set(Thread.currentThread());
                secondStarted.countDown();
                return WebTransportUtils.createBiStream(
                    connect, false, DefaultWebTransportSession.DEFAULT_BI_INITIALIZER);
              });
      await(secondStarted);
      second.get(5, TimeUnit.SECONDS);
      resumeRead.countDown();
      io.netty.util.concurrent.Future<?> firstStream =
          (io.netty.util.concurrent.Future<?>) first.get(5, TimeUnit.SECONDS);
      io.netty.util.concurrent.Future<?> secondStream =
          (io.netty.util.concurrent.Future<?>) second.get(5, TimeUnit.SECONDS);
      firstStream.await(5, TimeUnit.SECONDS);
      secondStream.await(5, TimeUnit.SECONDS);
      Field counter =
          DefaultWebTransportSession.class.getDeclaredField("serverInitiatedStreamsBidi");
      counter.setAccessible(true);
      Object val = counter.get(session);
      long used =
          (val instanceof Number)
              ? ((Number) val).longValue()
              : ((java.util.concurrent.atomic.AtomicLong) val).get();
      assertEquals("Peer limit=1", 1, used);
    } finally {
      resumeRead.countDown();
      executor.shutdownNow();
      loop.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
    }
  }

  @Test(timeout = 15000)
  public void pairedConfigurationReadsMustUseOneGeneration() throws Exception {
    Path config = Paths.get("webtransport-dynamic.properties");
    byte[] originalFile = Files.exists(config) ? Files.readAllBytes(config) : null;
    Field dynamic = WebTransportConfig.class.getDeclaredField("dynamicProperties");
    dynamic.setAccessible(true);
    Properties original = (Properties) dynamic.get(null);
    String prefix = "webtransport4j.server.ratelimit.concurrency.";
    try {
      Files.write(config, (prefix + "a=1\n" + prefix + "b=1\n").getBytes(StandardCharsets.UTF_8));
      WebTransportConfig.reload();
      WebTransportConfig.Snapshot snapshot = WebTransportConfig.snapshot();
      final String first = snapshot.get(prefix + "a", "missing");
      Files.write(config, (prefix + "a=2\n" + prefix + "b=2\n").getBytes(StandardCharsets.UTF_8));
      WebTransportConfig.reload();
      String second = snapshot.get(prefix + "b", "missing");
      assertEquals("2", WebTransportConfig.get(prefix + "b", "missing"));
      assertEquals("Separate reads crossed a real reload", first, second);
    } finally {
      dynamic.set(null, original);
      if (originalFile == null) {
        Files.deleteIfExists(config);
      } else {
        Files.write(config, originalFile);
      }
    }
  }

  @Test(timeout = 15000)
  public void olderConfigReloadMustNotOverwriteNewerPublication() throws Exception {
    Path config = Paths.get("webtransport-dynamic.properties");
    byte[] originalFile = Files.exists(config) ? Files.readAllBytes(config) : null;
    Field dynamic = WebTransportConfig.class.getDeclaredField("dynamicProperties");
    dynamic.setAccessible(true);
    Properties original = (Properties) dynamic.get(null);
    String key = "webtransport4j.server.ratelimit.concurrency.generation";
    CountDownLatch compared = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    AtomicBoolean first = new AtomicBoolean(true);
    Properties controlled =
        new Properties() {
          @Override
          public Object get(Object name) {
            if (first.compareAndSet(true, false)) {
              compared.countDown();
              await(resume);
            }
            return super.get(name);
          }
        };
    controlled.setProperty(key, "0");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      dynamic.set(null, controlled);
      Files.write(config, (key + "=1\n").getBytes(StandardCharsets.UTF_8));
      final Future<?> older = executor.submit(WebTransportConfig::reload);
      await(compared);
      Files.write(config, (key + "=2\n").getBytes(StandardCharsets.UTF_8));
      WebTransportConfig.reload();
      assertEquals("2", WebTransportConfig.get(key, "missing"));
      resume.countDown();
      older.get(5, TimeUnit.SECONDS);
      assertEquals(
          "Older reload replaced newer config", "2", WebTransportConfig.get(key, "missing"));
    } finally {
      resume.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      dynamic.set(null, original);
      if (originalFile == null) {
        Files.deleteIfExists(config);
      } else {
        Files.write(config, originalFile);
      }
    }
  }

  @Test(timeout = 30000)
  public void olderTlsReloadMustNotOverwriteNewerPublication() throws Exception {
    SelfSignedCertificate first = new SelfSignedCertificate("localhost");
    SelfSignedCertificate second = new SelfSignedCertificate("localhost");
    Path directory = Files.createTempDirectory("wt-race-tls-");
    Path key = directory.resolve("key.pem");
    Path cert = directory.resolve("cert.pem");
    WebTransportServer server = WebTransportServer.builder().port(0).build();
    Method install =
        WebTransportServer.class.getDeclaredMethod(
            "installReloadedSslContext", QuicSslContext.class);
    install.setAccessible(true);
    CountDownLatch built = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    AtomicBoolean firstCallback = new AtomicBoolean(true);
    AtomicReference<Thread> owner = new AtomicReference<>();
    AtomicReference<Thread> contender = new AtomicReference<>();
    AtomicReference<QuicSslContext> latest = new AtomicReference<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    TlsCertificateWatcher watcher =
        new TlsCertificateWatcher(
            key.toString(),
            cert.toString(),
            context -> {
              if (firstCallback.compareAndSet(true, false)) {
                owner.set(Thread.currentThread());
                built.countDown();
                await(resume);
              } else {
                latest.set(context);
              }
              try {
                install.invoke(server, context);
              } catch (Exception e) {
                throw new AssertionError(e);
              }
            });
    try {
      Files.copy(first.privateKey().toPath(), key);
      Files.copy(first.certificate().toPath(), cert);
      final Future<Boolean> older = executor.submit(watcher::checkAndReload);
      await(built);
      long nextTime =
          Math.max(
                  Files.getLastModifiedTime(key).toMillis(),
                  Files.getLastModifiedTime(cert).toMillis())
              + 2000;
      Files.copy(
          second.privateKey().toPath(), key, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      Files.copy(
          second.certificate().toPath(), cert, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      Files.setLastModifiedTime(key, FileTime.fromMillis(nextTime));
      Files.setLastModifiedTime(cert, FileTime.fromMillis(nextTime));
      CountDownLatch secondStarted = new CountDownLatch(1);
      ExecutorService secondCaller = Executors.newSingleThreadExecutor();
      try {
        final Future<Boolean> newer =
            secondCaller.submit(
                () -> {
                  contender.set(Thread.currentThread());
                  secondStarted.countDown();
                  return watcher.checkAndReload();
                });
        await(secondStarted);
        awaitCompletionOrBlockedBy(newer, contender.get(), owner.get());
        resume.countDown();
        assertTrue(older.get(5, TimeUnit.SECONDS));
        assertTrue(newer.get(5, TimeUnit.SECONDS));
      } finally {
        secondCaller.shutdownNow();
      }
      assertSame(
          "Older callback replaced newer TLS context", latest.get(), server.getActiveSslContext());
    } finally {
      resume.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      watcher.stop();
      server.close();
      first.delete();
      second.delete();
      Files.deleteIfExists(key);
      Files.deleteIfExists(cert);
      Files.deleteIfExists(directory);
    }
  }

  static final class Item implements AutoCloseable {
    final AtomicInteger closed = new AtomicInteger();

    public void close() {
      closed.incrementAndGet();
    }
  }
}

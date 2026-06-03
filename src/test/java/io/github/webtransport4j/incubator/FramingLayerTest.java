package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicChannel;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FramingLayerTest {

    @Test
    public void testDatagramFraming() {
        EmbeddedChannel channel = new EmbeddedChannel(new WebTransportDatagramHandler());

        ByteBuf input = Unpooled.buffer();
        // Write Session ID (varint 42)
        WebTransportUtils.writeVarInt(input, 42);
        // Write Payload "Hello"
        input.writeBytes("Hello".getBytes(StandardCharsets.UTF_8));

        assertTrue(channel.writeInbound(input));

        Object output = channel.readInbound();
        assertTrue(output instanceof WebTransportDatagramFrame);

        WebTransportDatagramFrame frame = (WebTransportDatagramFrame) output;
        assertEquals(42L, frame.sessionId());
        assertEquals("Hello", frame.content().toString(StandardCharsets.UTF_8));
        frame.release();
    }

    @Test
    public void testDataHandlerCapsuleParsing() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(WebTransportUtils.SESSION_ID_KEY).set(100L);
        channel.pipeline().addLast(new WebTransportDataHandler());

        final WebTransportCapsule[] received = new WebTransportCapsule[1];
        channel.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof WebTransportCapsule) {
                    received[0] = (WebTransportCapsule) msg;
                } else {
                    ctx.fireChannelRead(msg);
                }
            }
        });

        ByteBuf input = Unpooled.buffer();
        // Capsule Type = 0x2843 (CLOSE_WEBTRANSPORT_SESSION)
        WebTransportUtils.writeVarInt(input, 0x2843);
        // Length = 0
        WebTransportUtils.writeVarInt(input, 0);

        Http3DataFrame frame = new DefaultHttp3DataFrame(input);
        channel.writeInbound(frame);

        assertNotNull(received[0]);
        assertEquals(100L, received[0].sessionId());
        assertEquals(0x2843, received[0].capsuleType());
        received[0].release();
    }

    @Test
    public void testDataHandlerCapsuleFragmentation() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(WebTransportUtils.SESSION_ID_KEY).set(100L);
        channel.pipeline().addLast(new WebTransportDataHandler());

        final WebTransportCapsule[] received = new WebTransportCapsule[1];
        channel.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof WebTransportCapsule) {
                    received[0] = (WebTransportCapsule) msg;
                } else {
                    ctx.fireChannelRead(msg);
                }
            }
        });

        // Step 1: Send only 1 byte (incomplete capsule header)
        ByteBuf part1 = Unpooled.buffer();
        WebTransportUtils.writeVarInt(part1, 0x2843);
        int headerSize = part1.readableBytes();
        ByteBuf truncatedPart = part1.readSlice(headerSize - 1); // remove the last byte of header

        channel.writeInbound(new DefaultHttp3DataFrame(truncatedPart));
        assertNull(received[0]); // Verification: No capsule fired yet

        // Step 2: Send remaining bytes to complete the capsule header & value
        ByteBuf part2 = Unpooled.buffer();
        // Write the missing byte from previous varint (0x43)
        part2.writeByte(0x43); 
        // Write length 5
        WebTransportUtils.writeVarInt(part2, 5);
        // Write content "Hello"
        part2.writeBytes("Hello".getBytes(StandardCharsets.UTF_8));

        channel.writeInbound(new DefaultHttp3DataFrame(part2));
        assertNotNull(received[0]);
        assertEquals(100L, received[0].sessionId());
        assertEquals(0x2843, received[0].capsuleType());
        assertEquals("Hello", received[0].content().toString(StandardCharsets.UTF_8));
        received[0].release();
    }

    @Test
    public void testDataHandlerMultipleCapsules() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(WebTransportUtils.SESSION_ID_KEY).set(100L);
        channel.pipeline().addLast(new WebTransportDataHandler());

        final java.util.List<WebTransportCapsule> list = new java.util.ArrayList<>();
        channel.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (msg instanceof WebTransportCapsule) {
                    list.add((WebTransportCapsule) msg);
                } else {
                    ctx.fireChannelRead(msg);
                }
            }
        });

        ByteBuf input = Unpooled.buffer();
        // Capsule 1: DRAIN (0x2844), len = 2, value = "HI"
        WebTransportUtils.writeVarInt(input, 0x2844);
        WebTransportUtils.writeVarInt(input, 2);
        input.writeBytes("HI".getBytes(StandardCharsets.UTF_8));

        // Capsule 2: CLOSE (0x2843), len = 3, value = "BYE"
        WebTransportUtils.writeVarInt(input, 0x2843);
        WebTransportUtils.writeVarInt(input, 3);
        input.writeBytes("BYE".getBytes(StandardCharsets.UTF_8));

        channel.writeInbound(new DefaultHttp3DataFrame(input));

        assertEquals(2, list.size());
        assertEquals(0x2844, list.get(0).capsuleType());
        assertEquals("HI", list.get(0).content().toString(StandardCharsets.UTF_8));
        assertEquals(0x2843, list.get(1).capsuleType());
        assertEquals("BYE", list.get(1).content().toString(StandardCharsets.UTF_8));

        list.get(0).release();
        list.get(1).release();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStreamFrameDecoderDirectly() throws Exception {
        WebTransportStreamFrameDecoder decoder = new WebTransportStreamFrameDecoder();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        QuicStreamChannel mockStream = mock(QuicStreamChannel.class);

        when(mockCtx.channel()).thenReturn(mockStream);

        io.netty.util.Attribute<Long> typeAttr = mock(io.netty.util.Attribute.class);
        io.netty.util.Attribute<Long> sessIdAttr = mock(io.netty.util.Attribute.class);

        when(mockStream.attr(WebTransportUtils.STREAM_TYPE_KEY)).thenReturn(typeAttr);
        when(typeAttr.get()).thenReturn(0x41L); // BIDIRECTIONAL

        when(mockStream.attr(WebTransportUtils.SESSION_ID_KEY)).thenReturn(sessIdAttr);
        when(sessIdAttr.get()).thenReturn(42L);

        when(mockStream.streamId()).thenReturn(99L);

        ByteBuf input = Unpooled.copiedBuffer("Hello".getBytes(StandardCharsets.UTF_8));
        decoder.channelRead(mockCtx, input);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(mockCtx).fireChannelRead(captor.capture());

        Object output = captor.getValue();
        assertTrue(output instanceof WebTransportStreamFrame);
        WebTransportStreamFrame frame = (WebTransportStreamFrame) output;
        assertEquals(42L, frame.sessionId());
        assertEquals(99L, frame.streamId());
        assertTrue(frame.isBidirectional());
        assertEquals("Hello", frame.content().toString(StandardCharsets.UTF_8));

        frame.release();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMessageDispatcherStreamFrame() throws Exception {
        MessageDispatcher dispatcher = new MessageDispatcher();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
        QuicChannel mockParent = mock(QuicChannel.class);

        when(mockCtx.channel()).thenReturn(mockStream);
        when(mockStream.parent()).thenReturn(mockParent);
        when(mockStream.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

        io.netty.channel.EventLoop mockEventLoop = mock(io.netty.channel.EventLoop.class);
        when(mockStream.eventLoop()).thenReturn(mockEventLoop);
        doAnswer(invocation -> {
            Runnable r = invocation.getArgument(0);
            r.run();
            return null;
        }).when(mockEventLoop).execute(any(Runnable.class));

        io.netty.util.Attribute<java.util.concurrent.ExecutorService> execAttr = mock(io.netty.util.Attribute.class);
        when(mockParent.attr(WebTransportServer.BUSINESS_EXECUTOR)).thenReturn(execAttr);

        final boolean[] executed = new boolean[1];
        java.util.concurrent.ExecutorService directExecutor = new java.util.concurrent.AbstractExecutorService() {
            private boolean shutdown = false;
            @Override
            public void shutdown() { shutdown = true; }
            @Override
            public java.util.List<Runnable> shutdownNow() { shutdown = true; return java.util.Collections.emptyList(); }
            @Override
            public boolean isShutdown() { return shutdown; }
            @Override
            public boolean isTerminated() { return shutdown; }
            @Override
            public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) { return true; }
            @Override
            public void execute(Runnable command) {
                executed[0] = true;
                command.run(); // execute synchronously
            }
        };
        when(execAttr.get()).thenReturn(directExecutor);

        io.netty.util.Attribute<String> pathAttr = mock(io.netty.util.Attribute.class);
        when(mockParent.attr(WebTransportServer.SESSION_PATH_KEY)).thenReturn(pathAttr);
        when(pathAttr.get()).thenReturn("/test-path");

        ByteBuf data = Unpooled.copiedBuffer("App Message".getBytes(StandardCharsets.UTF_8));
        WebTransportStreamFrame frame = new WebTransportStreamFrame(101L, 202L, true, data);

        dispatcher.channelRead(mockCtx, frame);

        assertTrue(executed[0]);
        assertEquals(0, frame.refCnt()); // Verified: Memory safely recycled
    }

    @Test
    public void testMessageDispatcherCloseCapsuleSync() throws Exception {
        MessageDispatcher dispatcher = new MessageDispatcher();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        QuicStreamChannel mockStream = mock(QuicStreamChannel.class);

        when(mockCtx.channel()).thenReturn(mockStream);

        ByteBuf data = Unpooled.buffer(0);
        WebTransportCapsule closeCapsule = new WebTransportCapsule(101L, 0x2843L, data);

        dispatcher.channelRead(mockCtx, closeCapsule);

        // Verified: Closing happens synchronously on the EventLoop without offloading
        verify(mockCtx).close();
        assertEquals(0, closeCapsule.refCnt());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWebTransportHeadersHandlerHandshake() throws Exception {
        WebTransportHeadersHandler handler = new WebTransportHeadersHandler();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
        QuicChannel mockParent = mock(QuicChannel.class);

        when(mockCtx.channel()).thenReturn(mockStream);
        when(mockStream.parent()).thenReturn(mockParent);
        when(mockStream.streamId()).thenReturn(100L);

        io.netty.util.Attribute<Long> sessIdAttr = mock(io.netty.util.Attribute.class);
        when(mockStream.attr(WebTransportUtils.SESSION_ID_KEY)).thenReturn(sessIdAttr);

        io.netty.handler.codec.quic.QuicStreamChannelConfig mockConfig = mock(io.netty.handler.codec.quic.QuicStreamChannelConfig.class);
        when(mockStream.config()).thenReturn(mockConfig);
        when(mockConfig.isAutoRead()).thenReturn(true);

        io.netty.channel.ChannelPipeline mockPipeline = mock(io.netty.channel.ChannelPipeline.class);
        when(mockCtx.pipeline()).thenReturn(mockPipeline);
        when(mockPipeline.names()).thenReturn(java.util.Collections.emptyList());

        // Attributes on Parent (QuicChannel)
        io.netty.util.Attribute<java.util.List<String>> allowedOriginsAttr = mock(io.netty.util.Attribute.class);
        when(allowedOriginsAttr.get()).thenReturn(null); // allow all
        when(mockParent.attr(WebTransportServer.ALLOWED_ORIGINS)).thenReturn(allowedOriginsAttr);

        io.netty.util.Attribute<String> pathAttr = mock(io.netty.util.Attribute.class);
        when(mockParent.attr(WebTransportServer.SESSION_PATH_KEY)).thenReturn(pathAttr);

        WebTransportSessionManager mgr = new WebTransportSessionManager();
        io.netty.util.Attribute<WebTransportSessionManager> mgrAttr = mock(io.netty.util.Attribute.class);
        when(mgrAttr.get()).thenReturn(mgr);
        when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

        io.netty.util.Attribute<java.util.concurrent.atomic.AtomicLong> currentBidiAttr = mock(io.netty.util.Attribute.class);
        java.util.concurrent.atomic.AtomicLong currentBidi = new java.util.concurrent.atomic.AtomicLong(1L);
        when(currentBidiAttr.get()).thenReturn(currentBidi);
        when(mockParent.attr(WebTransportUtils.CURRENT_STREAMS_BIDI)).thenReturn(currentBidiAttr);

        io.netty.util.Attribute<java.util.concurrent.atomic.AtomicLong> currentUniAttr = mock(io.netty.util.Attribute.class);
        java.util.concurrent.atomic.AtomicLong currentUni = new java.util.concurrent.atomic.AtomicLong(0L);
        when(currentUniAttr.get()).thenReturn(currentUni);
        when(mockParent.attr(WebTransportUtils.CURRENT_STREAMS_UNI)).thenReturn(currentUniAttr);

        io.netty.util.Attribute<java.util.concurrent.atomic.AtomicLong> maxUniAttr = mock(io.netty.util.Attribute.class);
        when(maxUniAttr.get()).thenReturn(new java.util.concurrent.atomic.AtomicLong(100L));
        when(mockParent.attr(WebTransportUtils.SETTINGS_WT_INITIAL_MAX_STREAMS_UNI)).thenReturn(maxUniAttr);

        io.netty.util.Attribute<java.util.concurrent.atomic.AtomicLong> maxBidiAttr = mock(io.netty.util.Attribute.class);
        when(maxBidiAttr.get()).thenReturn(new java.util.concurrent.atomic.AtomicLong(100L));
        when(mockParent.attr(WebTransportUtils.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI)).thenReturn(maxBidiAttr);

        // Mock EventLoop and Promise for createUniStream / createBiStream
        io.netty.channel.EventLoop mockEventLoop = mock(io.netty.channel.EventLoop.class);
        when(mockParent.eventLoop()).thenReturn(mockEventLoop);
        io.netty.util.concurrent.Promise mockPromise = mock(io.netty.util.concurrent.Promise.class);
        when(mockEventLoop.newPromise()).thenReturn(mockPromise);
        when(mockPromise.addListener(any())).thenReturn(mockPromise);

        // Mock createStream for createUniStream / createBiStream
        io.netty.util.concurrent.Future mockFuture = mock(io.netty.util.concurrent.Future.class);
        when(mockParent.createStream(any(), any())).thenReturn(mockFuture);
        when(mockFuture.addListener(any())).thenReturn(mockFuture);

        // Http3HeadersFrame
        io.netty.handler.codec.http3.Http3HeadersFrame mockHeadersFrame = mock(io.netty.handler.codec.http3.Http3HeadersFrame.class);
        io.netty.handler.codec.http3.Http3Headers mockHeaders = new io.netty.handler.codec.http3.DefaultHttp3Headers();
        mockHeaders.method("CONNECT");
        mockHeaders.path("/webtransport-test");
        mockHeaders.set(":protocol", "webtransport-h3");
        when(mockHeadersFrame.headers()).thenReturn(mockHeaders);

        handler.channelRead(mockCtx, mockHeadersFrame);

        // Verify registration
        assertTrue(mgr.hasSession(100L));

        // Verify decrement of BIDI streams
        assertEquals(0L, currentBidi.get());

        // Verify response sent
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(mockCtx).writeAndFlush(captor.capture());
        io.netty.handler.codec.http3.Http3HeadersFrame respFrame = (io.netty.handler.codec.http3.Http3HeadersFrame) captor.getValue();
        assertEquals("200", respFrame.headers().status().toString());
    }
}

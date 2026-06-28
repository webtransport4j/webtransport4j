package io.github.webtransport4j.server;

import io.github.webtransport4j.api.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KeepAliveTest {

    @Before
    public void setUp() {
        System.setProperty("webtransport4j.server.keepalive.enabled", "true");
        System.setProperty("webtransport4j.server.keepalive.timeout.secs", "1");
    }

    @After
    public void tearDown() {
        System.clearProperty("webtransport4j.server.keepalive.enabled");
        System.clearProperty("webtransport4j.server.keepalive.timeout.secs");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHeartbeatInterceptionAndPong() throws Exception {
        RawWebTransportHandler handler = new RawWebTransportHandler();
        
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        QuicStreamChannel channel = mock(QuicStreamChannel.class);
        QuicChannel parent = mock(QuicChannel.class);
        
        when(ctx.channel()).thenReturn(channel);
        when(channel.parent()).thenReturn(parent);
        when(channel.streamId()).thenReturn(200L);
        when(channel.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));
        when(ctx.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

        // Attributes
        Attribute<Long> sessIdAttr = mock(Attribute.class);
        when(sessIdAttr.get()).thenReturn(100L);
        when(channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

        Attribute<Long> streamTypeAttr = mock(Attribute.class);
        when(streamTypeAttr.get()).thenReturn(WebTransportUtils.BI_STREAM_TYPE);
        when(channel.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);

        Attribute<Boolean> isHeartbeatAttr = mock(Attribute.class);
        final Boolean[] isHeartbeatRef = new Boolean[1];
        doAnswer(inv -> {
            isHeartbeatRef[0] = inv.getArgument(0);
            return null;
        }).when(isHeartbeatAttr).set(any());
        doAnswer(inv -> isHeartbeatRef[0]).when(isHeartbeatAttr).get();
        when(channel.attr(WebTransportAttributeKeys.IS_HEARTBEAT_STREAM)).thenReturn(isHeartbeatAttr);

        Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
        WebTransportSessionManager mgr = mock(WebTransportSessionManager.class);
        when(mgr.hasSession(100L)).thenReturn(true);
        when(mgrAttr.get()).thenReturn(mgr);
        when(parent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

        WebTransportSession session = mock(WebTransportSession.class);
        when(session.getActiveClientInitiatedBi()).thenReturn(new java.util.HashSet<>());
        when(session.incrementAndGetClientInitiatedStreamsBidi()).thenReturn(1L);
        when(session.getSettingsMaxStreamsBidi()).thenReturn(100L);
        when(mgr.get(100L)).thenReturn(session);

        // First write header (to consume it)
        ByteBuf header = Unpooled.buffer();
        WebTransportUtils.writeVarInt(header, WebTransportUtils.BI_STREAM_TYPE);
        WebTransportUtils.writeVarInt(header, 100);

        handler.channelRead(ctx, header);

        // Write 0x3F (Heartbeat indicator) followed by 0x01 (PING)
        ByteBuf pingPayload = Unpooled.buffer();
        pingPayload.writeByte(0x3F);
        pingPayload.writeByte(0x01);

        handler.channelRead(ctx, pingPayload);

        // Verify that IS_HEARTBEAT_STREAM is set to true
        assertEquals(Boolean.TRUE, isHeartbeatRef[0]);

        // Verify that PONG was written back (0x01)
        ArgumentCaptor<ByteBuf> writeCaptor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(ctx, times(1)).writeAndFlush(writeCaptor.capture());
        
        ByteBuf written = writeCaptor.getValue();
        assertEquals(1, written.readableBytes());
        assertEquals(0x01, written.readByte());
        written.release();

        // Verify that the body was NOT passed downstream to MessageDispatcher
        verify(ctx, never()).fireChannelRead(any());
        
        // Verify that session's last read time was updated
        verify(session, atLeastOnce()).updateLastReadTime();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSessionReaping() {
        WebTransportSessionManager mgr = new WebTransportSessionManager();

        QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
        QuicChannel mockParent = mock(QuicChannel.class);
        when(mockConnectStream.parent()).thenReturn(mockParent);
        when(mockConnectStream.streamId()).thenReturn(100L);
        when(mockConnectStream.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));

        io.netty.channel.EventLoop eventLoop = mock(io.netty.channel.EventLoop.class);
        when(mockParent.eventLoop()).thenReturn(eventLoop);

        Attribute<WebTransportServer> serverAttr = mock(Attribute.class);
        when(serverAttr.get()).thenReturn(new WebTransportServer());
        when(mockParent.attr(WebTransportAttributeKeys.SERVER_KEY)).thenReturn(serverAttr);

        // Setup channel attributes
        Attribute<Long> localLimitAttr = mock(Attribute.class);
        when(localLimitAttr.get()).thenReturn(10L);
        when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(localLimitAttr);
        when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI)).thenReturn(localLimitAttr);
        when(mockParent.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_DATA)).thenReturn(localLimitAttr);

        Attribute<Long> peerLimitAttr = mock(Attribute.class);
        when(peerLimitAttr.get()).thenReturn(10L);
        when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_BIDI)).thenReturn(peerLimitAttr);
        when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_STREAMS_UNI)).thenReturn(peerLimitAttr);
        when(mockParent.attr(WebTransportAttributeKeys.PEER_SETTINGS_MAX_DATA)).thenReturn(peerLimitAttr);

        Attribute<String> pathAttr = mock(Attribute.class);
        when(pathAttr.get()).thenReturn("/test-api");
        when(mockParent.attr(WebTransportAttributeKeys.SESSION_PATH_KEY)).thenReturn(pathAttr);

        Attribute<Long> sessIdAttr = mock(Attribute.class);
        when(mockConnectStream.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

        // Capture keepalive scheduled runnable
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);

        mgr.register(mockConnectStream);

        verify(eventLoop, times(1)).scheduleWithFixedDelay(
                taskCaptor.capture(),
                anyLong(),
                anyLong(),
                any()
        );

        Runnable keepAliveTask = taskCaptor.getValue();
        assertNotNull(keepAliveTask);

        // By default, session is active (lastReadTime is current time)
        // Run keepalive task, connection stream should NOT be closed
        keepAliveTask.run();
        verify(mockConnectStream, never()).shutdown(anyInt(), any());

        // Retrieve registered session
        WebTransportSession session = mgr.get(100L);
        assertNotNull(session);

        // Fast-forward lastReadTime to simulate timeout (2 seconds ago)
        // Since timeout threshold is 1 second, this will exceed it.
        session.updateLastReadTime();
        try {
            // We sleep 1.5 seconds to guarantee timeout threshold is exceeded
            Thread.sleep(1500);
        } catch (InterruptedException ignored) {}

        // Run keepalive task, connection stream should be closed
        keepAliveTask.run();
        verify(mockConnectStream, times(1)).shutdown(eq(WebTransportUtils.WT_SESSION_GONE), any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHeartbeatEchoMode() throws Exception {
        System.setProperty("webtransport4j.server.keepalive.mode", "ECHO");
        System.setProperty("webtransport4j.server.keepalive.ping.byte", "7");
        System.setProperty("webtransport4j.server.keepalive.pong.byte", "9");
        try {
            RawWebTransportHandler handler = new RawWebTransportHandler();
            
            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            QuicStreamChannel channel = mock(QuicStreamChannel.class);
            QuicChannel parent = mock(QuicChannel.class);
            
            when(ctx.channel()).thenReturn(channel);
            when(channel.parent()).thenReturn(parent);
            when(channel.streamId()).thenReturn(200L);
            when(channel.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));
            when(ctx.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

            // Attributes
            Attribute<Long> sessIdAttr = mock(Attribute.class);
            when(sessIdAttr.get()).thenReturn(100L);
            when(channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

            Attribute<Long> streamTypeAttr = mock(Attribute.class);
            when(streamTypeAttr.get()).thenReturn(WebTransportUtils.BI_STREAM_TYPE);
            when(channel.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);

            Attribute<Boolean> isHeartbeatAttr = mock(Attribute.class);
            final Boolean[] isHeartbeatRef = new Boolean[1];
            doAnswer(inv -> {
                isHeartbeatRef[0] = inv.getArgument(0);
                return null;
            }).when(isHeartbeatAttr).set(any());
            doAnswer(inv -> isHeartbeatRef[0]).when(isHeartbeatAttr).get();
            when(channel.attr(WebTransportAttributeKeys.IS_HEARTBEAT_STREAM)).thenReturn(isHeartbeatAttr);

            Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
            WebTransportSessionManager mgr = mock(WebTransportSessionManager.class);
            when(mgr.hasSession(100L)).thenReturn(true);
            when(mgrAttr.get()).thenReturn(mgr);
            when(parent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

            WebTransportSession session = mock(WebTransportSession.class);
            when(session.getActiveClientInitiatedBi()).thenReturn(new java.util.HashSet<>());
            when(session.incrementAndGetClientInitiatedStreamsBidi()).thenReturn(1L);
            when(session.getSettingsMaxStreamsBidi()).thenReturn(100L);
            when(mgr.get(100L)).thenReturn(session);

            // Handshake first
            ByteBuf header = Unpooled.buffer();
            WebTransportUtils.writeVarInt(header, WebTransportUtils.BI_STREAM_TYPE);
            WebTransportUtils.writeVarInt(header, 100);
            handler.channelRead(ctx, header);

            // Write 0x3F (Heartbeat indicator) followed by custom PING payload (e.g. sequence "ABCD")
            ByteBuf pingPayload = Unpooled.buffer();
            pingPayload.writeByte(0x3F);
            pingPayload.writeBytes("ABCD".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            handler.channelRead(ctx, pingPayload);

            // Verify that PONG was written back (should be "ABCD" exactly - Option B Echo)
            ArgumentCaptor<ByteBuf> writeCaptor = ArgumentCaptor.forClass(ByteBuf.class);
            verify(ctx, times(1)).writeAndFlush(writeCaptor.capture());
            
            ByteBuf written = writeCaptor.getValue();
            assertEquals("ABCD", written.toString(java.nio.charset.StandardCharsets.UTF_8));
            written.release();

        } finally {
            System.clearProperty("webtransport4j.server.keepalive.mode");
            System.clearProperty("webtransport4j.server.keepalive.ping.byte");
            System.clearProperty("webtransport4j.server.keepalive.pong.byte");
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDisabledKeepAlive() throws Exception {
        System.setProperty("webtransport4j.server.keepalive.enabled", "false");
        try {
            RawWebTransportHandler handler = new RawWebTransportHandler();
            
            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            QuicStreamChannel channel = mock(QuicStreamChannel.class);
            QuicChannel parent = mock(QuicChannel.class);
            
            when(ctx.channel()).thenReturn(channel);
            when(channel.parent()).thenReturn(parent);
            when(channel.streamId()).thenReturn(200L);
            when(channel.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));
            when(ctx.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

            // Attributes
            Attribute<Long> sessIdAttr = mock(Attribute.class);
            when(sessIdAttr.get()).thenReturn(100L);
            when(channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

            Attribute<Long> streamTypeAttr = mock(Attribute.class);
            when(streamTypeAttr.get()).thenReturn(WebTransportUtils.BI_STREAM_TYPE);
            when(channel.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);

            Attribute<Boolean> isHeartbeatAttr = mock(Attribute.class);
            final Boolean[] isHeartbeatRef = new Boolean[1];
            doAnswer(inv -> {
                isHeartbeatRef[0] = inv.getArgument(0);
                return null;
            }).when(isHeartbeatAttr).set(any());
            doAnswer(inv -> isHeartbeatRef[0]).when(isHeartbeatAttr).get();
            when(channel.attr(WebTransportAttributeKeys.IS_HEARTBEAT_STREAM)).thenReturn(isHeartbeatAttr);

            Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
            WebTransportSessionManager mgr = mock(WebTransportSessionManager.class);
            when(mgr.hasSession(100L)).thenReturn(true);
            when(mgrAttr.get()).thenReturn(mgr);
            when(parent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

            WebTransportSession session = mock(WebTransportSession.class);
            when(session.getActiveClientInitiatedBi()).thenReturn(new java.util.HashSet<>());
            when(session.incrementAndGetClientInitiatedStreamsBidi()).thenReturn(1L);
            when(session.getSettingsMaxStreamsBidi()).thenReturn(100L);
            when(mgr.get(100L)).thenReturn(session);

            // Handshake first
            ByteBuf header = Unpooled.buffer();
            WebTransportUtils.writeVarInt(header, WebTransportUtils.BI_STREAM_TYPE);
            WebTransportUtils.writeVarInt(header, 100);
            handler.channelRead(ctx, header);

            // Send 0x3F + data payload
            ByteBuf payload = Unpooled.buffer();
            payload.writeByte(0x3F);
            payload.writeByte(0x01);

            handler.channelRead(ctx, payload);

            // When disabled, IS_HEARTBEAT_STREAM should be set to false (or not set to true)
            assertNotEquals(Boolean.TRUE, isHeartbeatRef[0]);

            // Heartbeat response should NOT be written back
            verify(ctx, never()).writeAndFlush(any());

            // The body payload MUST be passed downstream (fired to MessageDispatcher)
            ArgumentCaptor<ByteBuf> fireCaptor = ArgumentCaptor.forClass(ByteBuf.class);
            verify(ctx, times(1)).fireChannelRead(fireCaptor.capture());
            
            ByteBuf fired = fireCaptor.getValue();
            assertEquals(2, fired.readableBytes());
            assertEquals(0x3F, fired.readByte());
            assertEquals(0x01, fired.readByte());
            fired.release();

        } finally {
            System.setProperty("webtransport4j.server.keepalive.enabled", "true");
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFragmentedMagicByteAndPing() throws Exception {
        RawWebTransportHandler handler = new RawWebTransportHandler();
        
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        QuicStreamChannel channel = mock(QuicStreamChannel.class);
        QuicChannel parent = mock(QuicChannel.class);
        
        when(ctx.channel()).thenReturn(channel);
        when(channel.parent()).thenReturn(parent);
        when(channel.streamId()).thenReturn(200L);
        when(channel.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));
        when(ctx.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

        // Attributes
        Attribute<Long> sessIdAttr = mock(Attribute.class);
        when(sessIdAttr.get()).thenReturn(100L);
        when(channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

        Attribute<Long> streamTypeAttr = mock(Attribute.class);
        when(streamTypeAttr.get()).thenReturn(WebTransportUtils.BI_STREAM_TYPE);
        when(channel.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);

        Attribute<Boolean> isHeartbeatAttr = mock(Attribute.class);
        final Boolean[] isHeartbeatRef = new Boolean[1];
        doAnswer(inv -> {
            isHeartbeatRef[0] = inv.getArgument(0);
            return null;
        }).when(isHeartbeatAttr).set(any());
        doAnswer(inv -> isHeartbeatRef[0]).when(isHeartbeatAttr).get();
        when(channel.attr(WebTransportAttributeKeys.IS_HEARTBEAT_STREAM)).thenReturn(isHeartbeatAttr);

        Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
        WebTransportSessionManager mgr = mock(WebTransportSessionManager.class);
        when(mgr.hasSession(100L)).thenReturn(true);
        when(mgrAttr.get()).thenReturn(mgr);
        when(parent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

        WebTransportSession session = mock(WebTransportSession.class);
        when(session.getActiveClientInitiatedBi()).thenReturn(new java.util.HashSet<>());
        when(session.incrementAndGetClientInitiatedStreamsBidi()).thenReturn(1L);
        when(session.getSettingsMaxStreamsBidi()).thenReturn(100L);
        when(mgr.get(100L)).thenReturn(session);

        // Handshake first
        ByteBuf header = Unpooled.buffer();
        WebTransportUtils.writeVarInt(header, WebTransportUtils.BI_STREAM_TYPE);
        WebTransportUtils.writeVarInt(header, 100);
        handler.channelRead(ctx, header);

        // Chunk 1: Send ONLY the magic byte 0x3F
        ByteBuf chunk1 = Unpooled.buffer();
        chunk1.writeByte(0x3F);
        handler.channelRead(ctx, chunk1);

        // Verify it is recognized as heartbeat stream
        assertEquals(Boolean.TRUE, isHeartbeatRef[0]);
        // No pong written yet (as no ping byte was sent)
        verify(ctx, never()).writeAndFlush(any());

        // Chunk 2: Send the PING byte 0x01
        ByteBuf chunk2 = Unpooled.buffer();
        chunk2.writeByte(0x01);
        handler.channelRead(ctx, chunk2);

        // Verify PONG is written back now
        ArgumentCaptor<ByteBuf> writeCaptor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(ctx, times(1)).writeAndFlush(writeCaptor.capture());
        
        ByteBuf written = writeCaptor.getValue();
        assertEquals(1, written.readableBytes());
        assertEquals(0x01, written.readByte());
        written.release();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultiplePingsInSinglePacket() throws Exception {
        RawWebTransportHandler handler = new RawWebTransportHandler();
        
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        QuicStreamChannel channel = mock(QuicStreamChannel.class);
        QuicChannel parent = mock(QuicChannel.class);
        
        when(ctx.channel()).thenReturn(channel);
        when(channel.parent()).thenReturn(parent);
        when(channel.streamId()).thenReturn(200L);
        when(channel.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));
        when(ctx.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

        // Attributes
        Attribute<Long> sessIdAttr = mock(Attribute.class);
        when(sessIdAttr.get()).thenReturn(100L);
        when(channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

        Attribute<Long> streamTypeAttr = mock(Attribute.class);
        when(streamTypeAttr.get()).thenReturn(WebTransportUtils.BI_STREAM_TYPE);
        when(channel.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);

        Attribute<Boolean> isHeartbeatAttr = mock(Attribute.class);
        final Boolean[] isHeartbeatRef = new Boolean[1];
        doAnswer(inv -> {
            isHeartbeatRef[0] = inv.getArgument(0);
            return null;
        }).when(isHeartbeatAttr).set(any());
        doAnswer(inv -> isHeartbeatRef[0]).when(isHeartbeatAttr).get();
        when(channel.attr(WebTransportAttributeKeys.IS_HEARTBEAT_STREAM)).thenReturn(isHeartbeatAttr);

        Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
        WebTransportSessionManager mgr = mock(WebTransportSessionManager.class);
        when(mgr.hasSession(100L)).thenReturn(true);
        when(mgrAttr.get()).thenReturn(mgr);
        when(parent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

        WebTransportSession session = mock(WebTransportSession.class);
        when(session.getActiveClientInitiatedBi()).thenReturn(new java.util.HashSet<>());
        when(session.incrementAndGetClientInitiatedStreamsBidi()).thenReturn(1L);
        when(session.getSettingsMaxStreamsBidi()).thenReturn(100L);
        when(mgr.get(100L)).thenReturn(session);

        // Handshake first
        ByteBuf header = Unpooled.buffer();
        WebTransportUtils.writeVarInt(header, WebTransportUtils.BI_STREAM_TYPE);
        WebTransportUtils.writeVarInt(header, 100);
        handler.channelRead(ctx, header);

        // Send 0x3F followed by 3 PING bytes in a single read
        ByteBuf multiPings = Unpooled.buffer();
        multiPings.writeByte(0x3F);
        multiPings.writeByte(0x01);
        multiPings.writeByte(0x01);
        multiPings.writeByte(0x01);

        handler.channelRead(ctx, multiPings);

        // Verify that 3 PONG responses were written out
        verify(ctx, times(3)).writeAndFlush(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHeartbeatStrictDifferentBytes() throws Exception {
        System.setProperty("webtransport4j.server.keepalive.ping.byte", "7");
        System.setProperty("webtransport4j.server.keepalive.pong.byte", "9");
        try {
            RawWebTransportHandler handler = new RawWebTransportHandler();
            
            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            QuicStreamChannel channel = mock(QuicStreamChannel.class);
            QuicChannel parent = mock(QuicChannel.class);
            
            when(ctx.channel()).thenReturn(channel);
            when(channel.parent()).thenReturn(parent);
            when(channel.streamId()).thenReturn(200L);
            when(channel.closeFuture()).thenReturn(mock(io.netty.channel.ChannelFuture.class));
            when(ctx.alloc()).thenReturn(io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);

            // Attributes
            Attribute<Long> sessIdAttr = mock(Attribute.class);
            when(sessIdAttr.get()).thenReturn(100L);
            when(channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY)).thenReturn(sessIdAttr);

            Attribute<Long> streamTypeAttr = mock(Attribute.class);
            when(streamTypeAttr.get()).thenReturn(WebTransportUtils.BI_STREAM_TYPE);
            when(channel.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY)).thenReturn(streamTypeAttr);

            Attribute<Boolean> isHeartbeatAttr = mock(Attribute.class);
            final Boolean[] isHeartbeatRef = new Boolean[1];
            doAnswer(inv -> {
                isHeartbeatRef[0] = inv.getArgument(0);
                return null;
            }).when(isHeartbeatAttr).set(any());
            doAnswer(inv -> isHeartbeatRef[0]).when(isHeartbeatAttr).get();
            when(channel.attr(WebTransportAttributeKeys.IS_HEARTBEAT_STREAM)).thenReturn(isHeartbeatAttr);

            Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
            WebTransportSessionManager mgr = mock(WebTransportSessionManager.class);
            when(mgr.hasSession(100L)).thenReturn(true);
            when(mgrAttr.get()).thenReturn(mgr);
            when(parent.attr(WebTransportAttributeKeys.WT_SESSION_MGR)).thenReturn(mgrAttr);

            WebTransportSession session = mock(WebTransportSession.class);
            when(session.getActiveClientInitiatedBi()).thenReturn(new java.util.HashSet<>());
            when(session.incrementAndGetClientInitiatedStreamsBidi()).thenReturn(1L);
            when(session.getSettingsMaxStreamsBidi()).thenReturn(100L);
            when(mgr.get(100L)).thenReturn(session);

            // Handshake first
            ByteBuf header = Unpooled.buffer();
            WebTransportUtils.writeVarInt(header, WebTransportUtils.BI_STREAM_TYPE);
            WebTransportUtils.writeVarInt(header, 100);
            handler.channelRead(ctx, header);

            // Write 0x3F (Heartbeat indicator) followed by incorrect ping byte 0x05, then correct ping byte 0x07
            ByteBuf pingPayload = Unpooled.buffer();
            pingPayload.writeByte(0x3F);
            pingPayload.writeByte(0x05); // Should be ignored
            pingPayload.writeByte(0x07); // Should trigger PONG (0x09)

            handler.channelRead(ctx, pingPayload);

            // Verify that exactly 1 PONG response was written out (the correct one)
            ArgumentCaptor<ByteBuf> writeCaptor = ArgumentCaptor.forClass(ByteBuf.class);
            verify(ctx, times(1)).writeAndFlush(writeCaptor.capture());
            
            ByteBuf written = writeCaptor.getValue();
            assertEquals(1, written.readableBytes());
            assertEquals(0x09, written.readByte()); // Should be the configured pong byte (9)
            written.release();

        } finally {
            System.clearProperty("webtransport4j.server.keepalive.ping.byte");
            System.clearProperty("webtransport4j.server.keepalive.pong.byte");
        }
    }
}

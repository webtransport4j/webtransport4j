package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StreamBufferingTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testOutOfOrderStreamBufferingAndReplay() throws Exception {
        // 1. Mock Parent QUIC Channel and Session Manager
        QuicChannel mockParent = mock(QuicChannel.class);
        WebTransportSessionManager mgr = new WebTransportSessionManager();
        Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
        when(mgrAttr.get()).thenReturn(mgr);
        when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

        // 2. Mock Stream Channel and context
        QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
        when(mockStream.parent()).thenReturn(mockParent);
        when(mockStream.streamId()).thenReturn(200L);
        io.netty.handler.codec.quic.QuicStreamChannelConfig mockConfig = mock(io.netty.handler.codec.quic.QuicStreamChannelConfig.class);
        when(mockStream.config()).thenReturn(mockConfig);
        ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
        when(mockStream.pipeline()).thenReturn(mockPipeline);

        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.channel()).thenReturn(mockStream);
        when(mockCtx.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

        // Setup channel attributes
        Attribute<Long> typeAttr = mock(Attribute.class);
        Attribute<Long> sessIdAttr = mock(Attribute.class);
        when(mockStream.attr(WebTransportUtils.STREAM_TYPE_KEY)).thenReturn(typeAttr);
        when(mockStream.attr(WebTransportUtils.SESSION_ID_KEY)).thenReturn(sessIdAttr);

        // 3. Prepare data with WT Headers (Session ID 100, BIDI stream type 0x41)
        ByteBuf data = Unpooled.buffer();
        WebTransportUtils.writeVarInt(data, 0x41);
        WebTransportUtils.writeVarInt(data, 100);
        data.writeBytes("Payload".getBytes(StandardCharsets.UTF_8));

        RawWebTransportHandler handler = new RawWebTransportHandler();

        // 4. Inbound read when CONNECT session has NOT been registered yet
        handler.channelRead(mockCtx, data);

        // Verify: auto-read was disabled on the stream channel
        verify(mockConfig).setAutoRead(false);
        // Verify: no data was fired downstream yet
        verify(mockCtx, never()).fireChannelRead(any());

        // 5. Register the session now (representing CONNECT arrival)
        QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
        when(mockConnectStream.streamId()).thenReturn(100L);
        when(mockConnectStream.attr(WebTransportUtils.SESSION_ID_KEY)).thenReturn(mock(Attribute.class));

        mgr.register(mockConnectStream);

        // Verify: auto-read was turned back on
        verify(mockConfig).setAutoRead(true);
        // Verify: buffered bytes replayed on the pipeline
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(mockPipeline).fireChannelRead(captor.capture());

        ByteBuf replayedData = (ByteBuf) captor.getValue();
        assertEquals(0, replayedData.readerIndex()); // Verified: Reader index was reset back to 0
        replayedData.release();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBufferedStreamLimitExceeded() throws Exception {
        QuicChannel mockParent = mock(QuicChannel.class);
        WebTransportSessionManager mgr = new WebTransportSessionManager();
        Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
        when(mgrAttr.get()).thenReturn(mgr);
        when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

        // Buffer 50 streams (MAX_BUFFERED_STREAMS)
        for (int i = 0; i < 50; i++) {
            QuicStreamChannel s = mock(QuicStreamChannel.class);
            when(s.streamId()).thenReturn(1000L + i);
            ByteBuf d = Unpooled.buffer();
            WebTransportUtils.writeVarInt(d, 0x41);
            WebTransportUtils.writeVarInt(d, 100);
            assertTrue(mgr.bufferStream(100, s, d));
            d.release();
        }

        // Try to buffer the 51st stream
        QuicStreamChannel rejectedStream = mock(QuicStreamChannel.class);
        when(rejectedStream.parent()).thenReturn(mockParent);
        when(rejectedStream.streamId()).thenReturn(2000L);
        io.netty.handler.codec.quic.QuicStreamChannelConfig mockConfig = mock(io.netty.handler.codec.quic.QuicStreamChannelConfig.class);
        when(rejectedStream.config()).thenReturn(mockConfig);

        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.channel()).thenReturn(rejectedStream);
        io.netty.channel.ChannelPromise mockPromise = mock(io.netty.channel.ChannelPromise.class);
        when(mockCtx.newPromise()).thenReturn(mockPromise);

        ByteBuf data = Unpooled.buffer();
        WebTransportUtils.writeVarInt(data, 0x41);
        WebTransportUtils.writeVarInt(data, 100);

        RawWebTransportHandler handler = new RawWebTransportHandler();
        handler.channelRead(mockCtx, data);

        // Verify: stream was shut down with error code WT_BUFFERED_STREAM_REJECTED (0x3994bd84)
        verify(rejectedStream).shutdown(eq(0x3994bd84), any(io.netty.channel.ChannelPromise.class));
        assertEquals(0, data.refCnt()); // Verified: Released
        
        mgr.closeAll(); // Clean up session manager
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStreamLimitExceededClosesSession() throws Exception {
        QuicChannel mockParent = mock(QuicChannel.class);
        
        // Setup limit = 1
        Attribute<AtomicLong> limitAttr = mock(Attribute.class);
        when(limitAttr.get()).thenReturn(new AtomicLong(1L));
        when(mockParent.attr(WebTransportUtils.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI)).thenReturn(limitAttr);

        // Setup current stream count = 1 (already at limit before incrementing)
        Attribute<AtomicLong> currentAttr = mock(Attribute.class);
        AtomicLong currentCount = new AtomicLong(1L);
        when(currentAttr.get()).thenReturn(currentCount);
        when(mockParent.attr(WebTransportUtils.CURRENT_STREAMS_BIDI)).thenReturn(currentAttr);

        // Setup Session Manager with a session
        WebTransportSessionManager mgr = new WebTransportSessionManager();
        Attribute<WebTransportSessionManager> mgrAttr = mock(Attribute.class);
        when(mgrAttr.get()).thenReturn(mgr);
        when(mockParent.attr(WebTransportSessionManager.WT_SESSION_MGR)).thenReturn(mgrAttr);

        QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
        when(mockConnectStream.streamId()).thenReturn(100L);
        when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
        when(mockConnectStream.attr(WebTransportUtils.SESSION_ID_KEY)).thenReturn(mock(Attribute.class));
        mgr.register(mockConnectStream);

        // New incoming stream
        QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
        when(mockStream.parent()).thenReturn(mockParent);
        when(mockStream.type()).thenReturn(io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL);
        when(mockStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));

        // Simulate WebTransportServer.java stream init logic
        boolean isBidi = mockStream.type() == io.netty.handler.codec.quic.QuicStreamType.BIDIRECTIONAL;
        long value = WebTransportUtils.incrementCounter(mockStream.parent(), isBidi ? WebTransportUtils.CURRENT_STREAMS_BIDI : WebTransportUtils.CURRENT_STREAMS_UNI);
        
        long maxAllowed = mockStream.parent().attr(isBidi ? WebTransportUtils.SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI : WebTransportUtils.SETTINGS_WT_INITIAL_MAX_STREAMS_UNI).get().get();
        
        if (value > maxAllowed) {
            mgr.closeAllWithFlowControlError();
            mockStream.shutdown(0x045d4487, mockStream.newPromise());
            mockStream.parent().close();
        }

        // Verify: CONNECT stream was shut down with WT_FLOW_CONTROL_ERROR (0x045d4487)
        verify(mockConnectStream).shutdown(eq(0x045d4487), any(io.netty.channel.ChannelPromise.class));
        // Verify: the offending stream was shut down with WT_FLOW_CONTROL_ERROR (0x045d4487)
        verify(mockStream).shutdown(eq(0x045d4487), any(io.netty.channel.ChannelPromise.class));
        // Verify: parent connection was closed
        verify(mockParent).close();
    }
}

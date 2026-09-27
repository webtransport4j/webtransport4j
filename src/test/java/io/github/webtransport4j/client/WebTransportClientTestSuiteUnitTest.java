package io.github.webtransport4j.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicException;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.codec.quic.QuicTransportError;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

/**
 * Unit tests verifying stream flow control and exception handling in WebTransportClientTestSuite.
 */
public class WebTransportClientTestSuiteUnitTest {

    @After
    public void tearDown() {
        System.clearProperty("webtransport4j.test.require_flow_control_exhaustion");
    }

    @Test
    public void testIsStreamLimitError() {
        assertFalse(WebTransportClientTestSuite.isStreamLimitError(null));
        assertFalse(WebTransportClientTestSuite.isStreamLimitError(new IOException("Connection reset by peer")));
        assertFalse(WebTransportClientTestSuite.isStreamLimitError(new ClosedChannelException()));

        // QuicException with STREAM_LIMIT_ERROR
        QuicException limitEx = new QuicException(QuicTransportError.STREAM_LIMIT_ERROR);
        assertTrue(WebTransportClientTestSuite.isStreamLimitError(limitEx));

        // QuicException with another error
        QuicException internalEx = new QuicException(QuicTransportError.INTERNAL_ERROR);
        assertFalse(WebTransportClientTestSuite.isStreamLimitError(internalEx));

        // Wrapped in ExecutionException
        assertTrue(WebTransportClientTestSuite.isStreamLimitError(new ExecutionException(limitEx)));

        // String messages containing stream limit
        assertTrue(WebTransportClientTestSuite.isStreamLimitError(new RuntimeException("QUICHE_ERR_STREAM_LIMIT")));
        assertTrue(WebTransportClientTestSuite.isStreamLimitError(new RuntimeException("stream limit reached")));
        assertTrue(WebTransportClientTestSuite.isStreamLimitError(new RuntimeException("STREAM_LIMIT_ERROR")));
    }

    @Test
    public void testFlowControlBlockedOnTimeout() throws Exception {
        System.setProperty("webtransport4j.test.require_flow_control_exhaustion", "true");
        QuicChannel mockChannel = mock(QuicChannel.class);
        @SuppressWarnings("unchecked")
        Future<QuicStreamChannel> mockFuture = mock(Future.class);

        when(mockChannel.createStream(eq(QuicStreamType.BIDIRECTIONAL), any())).thenReturn(mockFuture);
        // Timeout when awaiting stream creation
        when(mockFuture.await(1000, TimeUnit.MILLISECONDS)).thenReturn(false);

        // Should complete successfully because blocked was set to true due to timeout
        WebTransportClientTestSuite.testStreamFlowControl(mockChannel, 0L);
    }

    @Test
    public void testFlowControlBlockedOnStreamLimitRejection() throws Exception {
        System.setProperty("webtransport4j.test.require_flow_control_exhaustion", "true");
        QuicChannel mockChannel = mock(QuicChannel.class);
        @SuppressWarnings("unchecked")
        Future<QuicStreamChannel> mockFuture = mock(Future.class);

        when(mockChannel.createStream(eq(QuicStreamType.BIDIRECTIONAL), any())).thenReturn(mockFuture);
        when(mockFuture.await(1000, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(mockFuture.isSuccess()).thenReturn(false);
        when(mockFuture.cause()).thenReturn(new QuicException(QuicTransportError.STREAM_LIMIT_ERROR));

        // Should complete successfully because blocked was set to true due to stream-limit rejection
        WebTransportClientTestSuite.testStreamFlowControl(mockChannel, 0L);
    }

    @Test
    public void testFlowControlPropagatesUnrelatedException() {
        System.setProperty("webtransport4j.test.require_flow_control_exhaustion", "true");
        QuicChannel mockChannel = mock(QuicChannel.class);
        @SuppressWarnings("unchecked")
        Future<QuicStreamChannel> mockFuture = mock(Future.class);

        when(mockChannel.createStream(eq(QuicStreamType.BIDIRECTIONAL), any())).thenReturn(mockFuture);
        try {
            when(mockFuture.await(1000, TimeUnit.MILLISECONDS)).thenReturn(true);
        } catch (InterruptedException ignored) {
            // ignore
        }
        when(mockFuture.isSuccess()).thenReturn(false);
        ClosedChannelException closedEx = new ClosedChannelException();
        when(mockFuture.cause()).thenReturn(closedEx);

        try {
            WebTransportClientTestSuite.testStreamFlowControl(mockChannel, 0L);
            fail("Expected ClosedChannelException to be propagated");
        } catch (ClosedChannelException e) {
            // Success: unrelated exception was propagated rather than swallowed as blocked
        } catch (Exception e) {
            fail("Expected ClosedChannelException, but got: " + e);
        }
    }

    @Test
    public void testFlowControlPropagatesInterruptedException() {
        System.setProperty("webtransport4j.test.require_flow_control_exhaustion", "true");
        QuicChannel mockChannel = mock(QuicChannel.class);
        @SuppressWarnings("unchecked")
        Future<QuicStreamChannel> mockFuture = mock(Future.class);

        when(mockChannel.createStream(eq(QuicStreamType.BIDIRECTIONAL), any())).thenReturn(mockFuture);
        try {
            when(mockFuture.await(1000, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("interrupted test"));
        } catch (InterruptedException ignored) {
            // ignore
        }

        try {
            WebTransportClientTestSuite.testStreamFlowControl(mockChannel, 0L);
            fail("Expected InterruptedException to be propagated");
        } catch (InterruptedException e) {
            // Success: InterruptedException propagated and interrupted flag is preserved
            assertTrue("Thread interrupted flag should be set", Thread.interrupted());
        } catch (Exception e) {
            fail("Expected InterruptedException, but got: " + e);
        }
    }

    @Test
    public void testFlowControlThrowsWhenLimitNotReachedAndExhaustionRequired() {
        System.setProperty("webtransport4j.test.require_flow_control_exhaustion", "true");
        QuicChannel mockChannel = mock(QuicChannel.class);
        QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
        ChannelFuture mockCloseFuture = mock(ChannelFuture.class);
        when(mockStream.close()).thenReturn(mockCloseFuture);

        @SuppressWarnings("unchecked")
        Future<QuicStreamChannel> mockFuture = mock(Future.class);

        when(mockChannel.createStream(eq(QuicStreamType.BIDIRECTIONAL), any())).thenReturn(mockFuture);
        try {
            when(mockFuture.await(1000, TimeUnit.MILLISECONDS)).thenReturn(true);
        } catch (InterruptedException ignored) {
            // ignore
        }
        when(mockFuture.isSuccess()).thenReturn(true);
        when(mockFuture.getNow()).thenReturn(mockStream);

        try {
            WebTransportClientTestSuite.testStreamFlowControl(mockChannel, 0L);
            fail("Expected Exception when server limit was not reached within 150 streams");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("server limit was not reached"));
        }
    }
}

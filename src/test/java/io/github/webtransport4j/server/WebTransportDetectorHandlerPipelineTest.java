package io.github.webtransport4j.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

/**
 * Validates that {@link WebTransportDetectorHandler} preserves {@link WebTransportChunkedWriteHandler}
 * in the channel pipeline when hijacking for WebTransport bidirectional streams.
 */
public class WebTransportDetectorHandlerPipelineTest {

  @Test
  public void testHijackPipelinePreservesWebTransportChunkedWriteHandler() {
    WebTransportChunkedWriteHandler chunkedHandler = new WebTransportChunkedWriteHandler();
    ChannelInboundHandlerAdapter dummyHttp3Handler = new ChannelInboundHandlerAdapter();
    WebTransportDetectorHandler detectorHandler = new WebTransportDetectorHandler();

    EmbeddedChannel channel = new EmbeddedChannel();
    channel.pipeline().addLast("chunkedHandler", chunkedHandler);
    channel.pipeline().addLast("dummyHttp3Handler", dummyHttp3Handler);
    channel.pipeline().addLast("detectorHandler", detectorHandler);

    // Send WT_STREAM bidi stream frame header: 0x41 (65 decimal) followed by session ID varint
    ByteBuf in = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in, 0x41);
    WebTransportUtils.writeVarInt(in, 0); // Session ID

    channel.writeInbound(in);

    // WebTransportChunkedWriteHandler MUST be preserved in the pipeline
    assertNotNull(
        "WebTransportChunkedWriteHandler must be preserved in hijacked pipeline",
        channel.pipeline().get(WebTransportChunkedWriteHandler.class));

    // HTTP/3 handler should have been removed
    assertNull(
        "Dummy HTTP/3 handler should be removed from hijacked pipeline",
        channel.pipeline().get("dummyHttp3Handler"));

    channel.finishAndReleaseAll();
  }
}

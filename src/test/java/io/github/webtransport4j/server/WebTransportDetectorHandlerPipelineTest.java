package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

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
    assertNotNull(channel.pipeline().get(WebTransportChunkedWriteHandler.class), "WebTransportChunkedWriteHandler must be preserved in hijacked pipeline");

    // HTTP/3 handler should have been removed
    assertNull(channel.pipeline().get("dummyHttp3Handler"), "Dummy HTTP/3 handler should be removed from hijacked pipeline");

    channel.finishAndReleaseAll();
  }
}

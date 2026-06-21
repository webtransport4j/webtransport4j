package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class WebTransportDetectorHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger =
      Logger.getLogger(WebTransportDetectorHandler.class.getName());
  private boolean checked = false;

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (checked || !(msg instanceof ByteBuf)) {
      ctx.fireChannelRead(msg);
      return;
    }

    ByteBuf in = (ByteBuf) msg;

    // 1. Need at least 1 byte to make ANY decision
    if (in.readableBytes() < 1) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "📦 [SNIFFER] Bytes: "
              + in.readableBytes()
              + " | HEX: ["
              + ByteBufUtil.hexDump(in)
              + "]");
    }

    int firstByte = in.getUnsignedByte(in.readerIndex());

    // 2. HTTP/3 Detection
    boolean isStandardHttp3 =
        (firstByte == 0x00
            || // DATA
            firstByte == 0x01
            || // HEADERS
            firstByte == 0x03
            || // CANCEL_PUSH
            firstByte == 0x04
            || // SETTINGS
            firstByte == 0x05
            || // PUSH_PROMISE
            firstByte == 0x07
            || // GOAWAY
            firstByte == 0x0d // MAX_PUSH_ID
        );

    if (isStandardHttp3) {
      logger.debug("👉 Decision: Standard HTTP/3 (0x" + Integer.toHexString(firstByte) + ")");
      checked = true;
      ctx.pipeline().remove(this);
      ctx.fireChannelRead(msg);
      return;
    }

    // 3. WebTransport Detection (0x40 + 0x41)
    if (in.readableBytes() < 2) {
      return; // Wait for more bytes
    }

    int secondByte = in.getUnsignedByte(in.readerIndex() + 1);
    boolean isWebTransportBidi = (firstByte == 0x40 && secondByte == 0x41);

    if (isWebTransportBidi) {
      logger.info("🚀 Decision: Raw WebTransport Stream Detected! Hijacking pipeline.");
      checked = true;
      hijackPipeline(ctx);

      ctx.fireChannelRead(msg);
    } else {
      logger.debug(
          "👉 Decision: Pass-through (0x"
              + Integer.toHexString(firstByte)
              + " "
              + Integer.toHexString(secondByte)
              + ")");
      checked = true;
      ctx.pipeline().remove(this);
      ctx.fireChannelRead(msg);
    }
  }

  private void hijackPipeline(ChannelHandlerContext ctx) {
    ChannelPipeline p = ctx.pipeline();
    List<String> toRemove = new ArrayList<>();

    for (String name : p.names()) {
      ChannelHandler h = p.get(name);
      if (h == null) continue;
      if (h == this) {
        toRemove.add(name);
        continue;
      }
      if (h instanceof QuicGlobalSniffer
          || h instanceof RawWebTransportHandler
          || h instanceof WebTransportStreamFrameDecoder
          || h instanceof MessageDispatcher
          || h instanceof GlobalTrafficShapingHandler
          || h instanceof ChannelTrafficShapingHandler) {
        continue;
      }
      toRemove.add(name);
    }

    for (String name : toRemove) {
      p.remove(name);
      logger.debug("   🗑 Removed: " + name);
    }
  }
}

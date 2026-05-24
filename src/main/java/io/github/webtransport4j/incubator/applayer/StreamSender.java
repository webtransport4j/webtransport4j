package io.github.webtransport4j.incubator.applayer;

import org.apache.log4j.Logger;

/**
 * @author https://github.com/sanjomo
 * @date 20/01/26 1:25 am
 */
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.CharsetUtil;

public class StreamSender {
    private static final Logger logger = Logger.getLogger(StreamSender.class.getName());
    private final Channel streamChannel;

    public StreamSender(Channel streamChannel) {
        this.streamChannel = streamChannel;
    }

    public Channel getStreamChannel() {
        return streamChannel;
    }

    // Write data to the EXISTING stream
    public void send(String payload) {
        if (streamChannel.isActive()) {
            streamChannel.writeAndFlush(Unpooled.copiedBuffer(payload, CharsetUtil.UTF_8)).addListener(future -> {
                if (!future.isSuccess()) {
                    Throwable cause = future.cause();
                    String causeName = cause.getClass().getName();
                    if (cause instanceof java.nio.channels.ClosedChannelException ||
                            causeName.contains("ClosedChannel") ||
                            causeName.contains("Timeout")) {
                        logger.debug("Push failed because channel is closed/timed out: " + cause.getMessage());
                    } else {
                        logger.error("❌ Push Failed: ", cause);
                    }
                } else {
                    logger.debug("✅ Push Sent: " + payload);
                }
            });
        } else {
            logger.debug("❌ Stream is closed, cannot push.");
        }
    }

    public void close() {
        streamChannel.close();
    }
}
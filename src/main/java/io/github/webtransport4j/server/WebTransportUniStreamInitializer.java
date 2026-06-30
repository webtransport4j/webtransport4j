package io.github.webtransport4j.server;

/*
 * @author https://github.com/sanjomo
 * @date 24/06/26 2:05 pm
 */
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;

/** Initializer for unidirectional WebTransport streams. */
public final class WebTransportUniStreamInitializer extends ChannelInitializer<QuicStreamChannel> {

  private final long streamType;

  public WebTransportUniStreamInitializer(long streamType) {
    this.streamType = streamType;
  }

  @Override
  protected void initChannel(@NonNull QuicStreamChannel ch) {
    WebTransportUtils.addTrafficShapers(ch);
    ch.pipeline().addLast(new WebTransportUniStreamHeaderDecoder(this.streamType));
    ch.pipeline().addLast(new WebTransportStreamFrameDecoder());
    ch.pipeline().addLast(new WebTransportCapsuleHandler());
    Supplier<MessageDispatcher> supplier =
        ch.parent().attr(WebTransportAttributeKeys.MESSAGE_DISPATCHER_SUPPLIER).get();
    if (supplier != null) {
      ch.pipeline().addLast(supplier.get());
    } else {
      ch.pipeline().addLast(new DefaultMessageDispatcher());
    }
  }
}

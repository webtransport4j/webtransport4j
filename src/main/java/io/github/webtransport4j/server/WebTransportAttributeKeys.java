package io.github.webtransport4j.server;

import io.github.webtransport4j.api.WebTransportMetricsListener;
import io.github.webtransport4j.api.WebTransportStream;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.AttributeKey;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** Netty channel attribute keys for WebTransport. */
public final class WebTransportAttributeKeys {

  private WebTransportAttributeKeys() {}

  // Session-related Attribute Keys
  public static final AttributeKey<WebTransportServer> SERVER_KEY =
      AttributeKey.valueOf("wt.server.instance");

  public static final AttributeKey<Long> SESSION_ID_KEY = AttributeKey.valueOf("wt.session.id");

  public static final AttributeKey<String> SESSION_PATH_KEY =
      AttributeKey.valueOf("wt.session.path.key");

  public static final AttributeKey<WebTransportSessionManager> WT_SESSION_MGR =
      AttributeKey.valueOf("wt.session.manager");

  public static final AttributeKey<Boolean> PEER_SETTINGS_RECEIVED =
      AttributeKey.valueOf("wt.peer.settings.received");

  public static final AttributeKey<Boolean> PEER_SETTINGS_VALID =
      AttributeKey.valueOf("wt.peer.settings.valid");

  public static final AttributeKey<ExecutorService> BUSINESS_EXECUTOR =
      AttributeKey.valueOf("wt.business.executor");

  public static final AttributeKey<List<String>> ALLOWED_ORIGINS =
      AttributeKey.valueOf("wt.allowed.origins");

  public static final AttributeKey<java.util.concurrent.atomic.AtomicInteger> GLOBAL_SESSION_COUNT =
      AttributeKey.valueOf("wt.global.session.count");

  // Stream-related Attribute Keys
  public static final AttributeKey<Long> STREAM_TYPE_KEY = AttributeKey.valueOf("wt.stream.type");

  public static final AttributeKey<Boolean> SERVER_INITIATED_KEY =
      AttributeKey.valueOf("wt.stream.server_initiated");

  public static final AttributeKey<WebTransportStream> WT_STREAM_KEY =
      AttributeKey.valueOf("wt.stream.instance");

  public static final AttributeKey<Boolean> STREAM_NOTIFIED =
      AttributeKey.valueOf("wt.stream.notified");

  public static final AttributeKey<Boolean> IS_HEARTBEAT_STREAM =
      AttributeKey.valueOf("wt.stream.is_heartbeat");

  // Flow Control Settings Attribute Keys
  public static final AttributeKey<Long> LOCAL_SETTINGS_MAX_STREAMS_UNI =
      AttributeKey.valueOf("wt.local.max_streams_uni");

  public static final AttributeKey<Long> LOCAL_SETTINGS_MAX_STREAMS_BIDI =
      AttributeKey.valueOf("wt.local.max_streams_bidi");

  public static final AttributeKey<Long> LOCAL_SETTINGS_MAX_DATA =
      AttributeKey.valueOf("wt.local.max_data");

  public static final AttributeKey<Long> PEER_SETTINGS_MAX_STREAMS_UNI =
      AttributeKey.valueOf("wt.peer.max_streams_uni");

  public static final AttributeKey<Long> PEER_SETTINGS_MAX_STREAMS_BIDI =
      AttributeKey.valueOf("wt.peer.max_streams_bidi");

  public static final AttributeKey<Long> PEER_SETTINGS_MAX_DATA =
      AttributeKey.valueOf("wt.peer.max_data");

  // Connection Traffic Shaping Attribute Key
  public static final AttributeKey<GlobalTrafficShapingHandler> CONN_TRAFFIC_SHAPER =
      AttributeKey.valueOf("wt.conn.traffic.shaper");

  public static final AttributeKey<GlobalTrafficShapingHandler> GLOBAL_TRAFFIC_SHAPER =
      AttributeKey.valueOf("wt.global.traffic.shaper");

  // Observability metrics listener — attached at server start and propagated to all QuicChannels
  public static final AttributeKey<WebTransportMetricsListener> METRICS_LISTENER =
      AttributeKey.valueOf("wt.metrics.listener");

  // Message dispatcher factory
  public static final AttributeKey<java.util.function.Supplier<MessageDispatcher>>
      MESSAGE_DISPATCHER_SUPPLIER = AttributeKey.valueOf("wt.message.dispatcher.supplier");
}

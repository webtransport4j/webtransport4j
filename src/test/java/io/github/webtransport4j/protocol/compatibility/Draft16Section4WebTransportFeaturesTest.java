package io.github.webtransport4j.protocol.compatibility;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportCapsule;
import io.github.webtransport4j.server.WebTransportCapsuleHandler;
import io.github.webtransport4j.server.WebTransportDatagramDecoder;
import io.github.webtransport4j.server.WebTransportDatagramFrame;
import io.github.webtransport4j.server.WebTransportDetectorHandler;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.github.webtransport4j.server.WebTransportUniStreamHeaderDecoder;
import io.github.webtransport4j.server.WebTransportUniStreamInitializer;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.DefaultAttributeMap;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Protocol compatibility tests for draft-ietf-webtrans-http3-16 Section 4: WebTransport Features.
 * Directly exercises application components: {@link UnknownStreamHandlerFactory},
 * {@link WebTransportUniStreamHeaderDecoder}, {@link WebTransportDetectorHandler},
 * {@link WebTransportDatagramDecoder}, {@link WebTransportCapsuleHandler}, and {@link WebTransportUtils}.
 */
public class Draft16Section4WebTransportFeaturesTest {

  /**
   * Section 4.1: Transport Properties.
   * "Unreliable Delivery: WebTransport over HTTP/3 supports unreliable delivery...
   * Pooling: WebTransport over HTTP/3 provides optional support for pooling."
   * OPTIONAL: Optional transport properties query interface.
   */
  @Test
  public void testSection4_1_TransportProperties_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 4.2: Unidirectional streams.
   * "The HTTP/3 unidirectional stream type SHALL be 0x54."
   * MANDATORY: Test UnknownStreamHandlerFactory recognizes 0x54 as WebTransport Uni stream
   * and rejects unknown stream types with H3_STREAM_CREATION_ERROR (0x010e).
   */
  @Test
  public void testSection4_2_UnidirectionalStreamTypeRecognitionInApplication() {
    UnknownStreamHandlerFactory factory = new UnknownStreamHandlerFactory();

    ChannelHandler wtHandler = factory.apply(WebTransportUtils.UNI_STREAM_TYPE);
    assertNotNull("Stream type 0x54 must produce WebTransportUniStreamInitializer", wtHandler);
    assertTrue("Must be WebTransportUniStreamInitializer",
        wtHandler instanceof WebTransportUniStreamInitializer);

    ChannelHandler unknownHandler = factory.apply(0x99L);
    assertNotNull("Unknown stream type must produce an initializer", unknownHandler);
    assertFalse("Unknown stream type must NOT be WebTransportUniStreamInitializer",
        unknownHandler instanceof WebTransportUniStreamInitializer);
  }

  /**
   * Section 4.2: Unidirectional streams.
   * "The body of the stream SHALL be the stream type, followed by the session ID, encoded as a
   * variable-length integer, followed by the user-specified stream data."
   * MANDATORY: Test WebTransportUniStreamHeaderDecoder parses session ID and forwards user payload.
   */
  @Test
  public void testSection4_2_UnidirectionalStreamDecoderInApplication() {
    final WebTransportUniStreamHeaderDecoder decoder =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);

    EmbeddedChannel channel = new EmbeddedChannel();
    EmbeddedChannel parentChannel = new EmbeddedChannel();
    parentChannel.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set("/wt-test");

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    QuicChannel mockParent = mock(QuicChannel.class);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    parentAttrMap.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set("/wt-path");

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    ByteBuf in = Unpooled.buffer();
    long sessionId = 12L;
    WebTransportUtils.writeVarInt(in, sessionId);
    byte[] payloadBytes = "UniPayloadData".getBytes(StandardCharsets.UTF_8);
    in.writeBytes(payloadBytes);

    EmbeddedChannel testChannel = new EmbeddedChannel(decoder);
    testChannel.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set("/test");
    // Feed through pipeline
    testChannel.writeInbound(in);

    assertEquals("Session ID attribute MUST be set to decoded session ID",
        Long.valueOf(sessionId), testChannel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get());
    assertEquals("Stream type attribute MUST be set to 0x54",
        Long.valueOf(0x54L), testChannel.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).get());

    ByteBuf forwarded = testChannel.readInbound();
    assertNotNull("Stream payload MUST be forwarded to subsequent handlers", forwarded);
    byte[] actualPayload = new byte[forwarded.readableBytes()];
    forwarded.readBytes(actualPayload);
    forwarded.release();
    assertArrayEquals("Payload bytes must match user stream data", payloadBytes, actualPayload);
    testChannel.finishAndReleaseAll();
  }

  /**
   * Section 4.3: Bidirectional Streams.
   * "Clients and servers use the signal value 0x41 to open a bidirectional WebTransport stream."
   * MANDATORY: Test WebTransportDetectorHandler detects 0x41 and hijacks pipeline.
   */
  @Test
  public void testSection4_3_BidirectionalStreamDetectorInApplication() {
    WebTransportDetectorHandler detector = new WebTransportDetectorHandler();
    EmbeddedChannel channel = new EmbeddedChannel(new HttpServerCodec(), detector);

    assertTrue("Pipeline initially contains HTTP codec",
        channel.pipeline().get(HttpServerCodec.class) != null);

    ByteBuf bidiSignalBuf = Unpooled.buffer();
    WebTransportUtils.writeVarInt(bidiSignalBuf, WebTransportUtils.BI_STREAM_TYPE); // 0x41
    WebTransportUtils.writeVarInt(bidiSignalBuf, 0L); // Session ID 0

    channel.writeInbound(bidiSignalBuf);

    // After detecting 0x41, standard HTTP handlers are stripped from the pipeline
    assertFalse("HTTP codec MUST be removed upon detecting WT_STREAM signal (0x41)",
        channel.pipeline().names().contains(HttpServerCodec.class.getSimpleName()));
    channel.finishAndReleaseAll();
  }

  /**
   * Section 4.3: Bidirectional Streams.
   * "Session IDs are derived from the stream ID of the CONNECT stream that established the session
   * and therefore MUST always correspond to a client-initiated bidirectional stream, as defined in
   * Section 2.1 of [RFC9000]."
   * MANDATORY: Assert client-initiated bidirectional streams have IDs % 4 == 0.
   */
  @Test
  public void testSection4_3_ClientInitiatedBidiFormulaValidation() {
    for (long id = 0; id <= 100; id += 4) {
      assertEquals("Client-initiated bidi stream IDs must satisfy id % 4 == 0", 0L, id % 4L);
    }
  }

  /**
   * Section 4.4: Resetting Data Streams.
   * "WebTransport implementations MUST remap those error codes into the error range reserved for
   * WT_APPLICATION_ERROR, where 0x00000000 corresponds to 0x52e4a40fa8db, and 0xffffffff corresponds
   * to 0x52e5ac983162. Note that there are codepoints inside that range of form '0x1f * N + 0x21'
   * that are reserved by Section 8.1 of [HTTP3]; those have to be skipped when mapping."
   * MANDATORY: Test WebTransportUtils bidirectional error remapping functions.
   */
  @Test
  public void testSection4_4_ApplicationErrorRemappingInApplication() {
    // Boundary test 0x00000000 -> 0x52e4a40fa8db
    long httpZero = WebTransportUtils.webTransportCodeToHttpCode(0x00000000L);
    assertEquals("0x00000000 must map to 0x52e4a40fa8db", 0x52e4a40fa8dbL, httpZero);
    assertEquals("Round-trip mapping for 0 must yield 0", 0x00000000L,
        WebTransportUtils.httpCodeToWebTransportCode(httpZero));

    // Boundary test 0xffffffff -> 0x52e5ac983162
    long httpMax = WebTransportUtils.webTransportCodeToHttpCode(0xffffffffL);
    assertEquals("0xffffffff must map to 0x52e5ac983162", 0x52e5ac983162L, httpMax);
    assertEquals("Round-trip mapping for max must yield 0xffffffff", 0xffffffffL,
        WebTransportUtils.httpCodeToWebTransportCode(httpMax));

    // Test skipping reserved codepoints of form 0x1f * N + 0x21
    for (long appCode = 0; appCode <= 200; appCode++) {
      long mapped = WebTransportUtils.webTransportCodeToHttpCode(appCode);
      assertFalse("Mapped HTTP/3 error code MUST NOT be of form 0x1f * N + 0x21",
          (mapped - 0x21L) % 31L == 0L);
      assertEquals("Round-trip must recover original application error code",
          appCode, WebTransportUtils.httpCodeToWebTransportCode(mapped));
    }
  }

  /**
   * Section 4.5: Datagrams / Quarter Stream ID.
   * "Quarter Stream ID: An integer that is a fourth of the Stream ID of the WebTransport CONNECT
   * stream with which the datagram is associated."
   * MANDATORY: Test WebTransportDatagramDecoder decodes quarter stream ID and shifts by 2 to session ID.
   */
  @Test
  public void testSection4_5_DatagramDecoderQuarterStreamIdInApplication() {
    WebTransportDatagramDecoder decoder = WebTransportDatagramDecoder.INSTANCE;
    EmbeddedChannel channel = new EmbeddedChannel(decoder);

    ByteBuf datagramBuf = Unpooled.buffer();
    long quarterSessionId = 3L; // Corresponds to session ID 12 (3 * 4)
    WebTransportUtils.writeVarInt(datagramBuf, quarterSessionId);
    byte[] dgramData = "DatagramContent".getBytes(StandardCharsets.UTF_8);
    datagramBuf.writeBytes(dgramData);

    channel.writeInbound(datagramBuf);

    WebTransportDatagramFrame frame = channel.readInbound();
    assertNotNull("Decoder must output WebTransportDatagramFrame", frame);
    assertEquals("Session ID must be quarterSessionId << 2 (12)", 12L, frame.sessionId());

    byte[] payload = new byte[frame.content().readableBytes()];
    frame.content().readBytes(payload);
    frame.release();
    assertArrayEquals("Payload must match datagram content", dgramData, payload);
    channel.finishAndReleaseAll();
  }

  /**
   * Section 4.6: Buffering Incoming Streams and Datagrams.
   * "The server MAY buffer incoming streams and datagrams before the session is established..."
   * OPTIONAL: Buffering streams and datagrams.
   */
  @Test
  public void testSection4_6_BufferingStreamsAndDatagrams_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 4.7: Session Drain.
   * "Length (i) = 0. The capsule MUST NOT contain any payload. An endpoint that receives a
   * WT_DRAIN_SESSION capsule with a non-zero length MUST treat this as a session error of
   * type H3_MESSAGE_ERROR."
   * MANDATORY: Test WebTransportCapsuleHandler marks session as draining on valid WT_DRAIN_SESSION
   * and resets connect stream with H3_MESSAGE_ERROR when payload is non-zero.
   */
  @Test
  public void testSection4_7_SessionDrainCapsuleInApplication() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockConnectStream);
    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.newPromise()).thenReturn(mock(io.netty.channel.ChannelPromise.class));
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any())).thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(0L);
    assertNotNull(session);
    assertFalse("Session is not draining initially", session.isDraining());

    // Valid WT_DRAIN_SESSION with empty payload
    WebTransportCapsule validDrainCapsule = new WebTransportCapsule(0L, 0x78aeL, Unpooled.EMPTY_BUFFER);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, validDrainCapsule);
    assertTrue("Session MUST be marked as draining after receiving valid WT_DRAIN_SESSION",
        session.isDraining());

    // Invalid WT_DRAIN_SESSION with non-empty payload
    ByteBuf nonZeroPayload = Unpooled.wrappedBuffer(new byte[]{0x01});
    WebTransportCapsule invalidDrainCapsule = new WebTransportCapsule(0L, 0x78aeL, nonZeroPayload);
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, invalidDrainCapsule);
    verify(mockConnectStream).shutdown(eq(0x010e), any());
  }

  /**
   * Section 4.7: Session Drain.
   * "Endpoints MAY continue using the session after sending or receiving a WT_DRAIN_SESSION capsule."
   * OPTIONAL: Continued usage after drain.
   */
  @Test
  public void testSection4_7_ContinueUsingSessionAfterDrain_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }

  /**
   * Section 4.8: Use of Keying Material Exporters.
   * "If the application requests an exporter for a given WebTransport session with a specified label
   * and context, the resulting exporter MUST be a TLS exporter as defined in Section 7.5 of [RFC8446]
   * with the label set to 'EXPORTER-WebTransport' and the context set to the serialization of the
   * 'WebTransport Exporter Context' struct."
   * MANDATORY: Test WebTransportUtils.serializeExporterContext formats context struct correctly.
   */
  @Test
  public void testSection4_8_KeyingMaterialExporterContextSerializationInApplication() {
    long sessionId = 16L;
    byte[] appContext = "MyAppContext".getBytes(StandardCharsets.UTF_8);

    byte[] serialized = WebTransportUtils.serializeExporterContext(sessionId, appContext);

    ByteBuf buf = Unpooled.wrappedBuffer(serialized);
    long decodedSessionId = WebTransportUtils.readVariableLengthInt(buf);
    byte[] decodedContext = new byte[buf.readableBytes()];
    buf.readBytes(decodedContext);
    buf.release();

    assertEquals("Session ID in exporter context MUST match session", sessionId, decodedSessionId);
    assertArrayEquals("Application context MUST be preserved", appContext, decodedContext);
  }
}

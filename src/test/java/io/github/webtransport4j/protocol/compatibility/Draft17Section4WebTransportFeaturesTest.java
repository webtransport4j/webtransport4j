package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.webtransport4j.api.WebTransportSession;
import io.github.webtransport4j.api.WebTransportStreamResetException;
import io.github.webtransport4j.server.DefaultMessageDispatcher;
import io.github.webtransport4j.server.DefaultNettyWebTransportStream;
import io.github.webtransport4j.server.Http3InboundControlStreamHandler;
import io.github.webtransport4j.server.Http3WtStreamFrameGuard;
import io.github.webtransport4j.server.StreamResetTransport;
import io.github.webtransport4j.server.UnknownStreamHandlerFactory;
import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportCapsule;
import io.github.webtransport4j.server.WebTransportCapsuleHandler;
import io.github.webtransport4j.server.WebTransportDatagramDecoder;
import io.github.webtransport4j.server.WebTransportDatagramFrame;
import io.github.webtransport4j.server.WebTransportDetectorHandler;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportKeyExporter;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportSessionManager;
import io.github.webtransport4j.server.WebTransportUniStreamHeaderDecoder;
import io.github.webtransport4j.server.WebTransportUniStreamInitializer;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http3.DefaultHttp3UnknownFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Mocked unit tests for {@code draft-ietf-webtrans-http3-17} Section 4: WebTransport Features.
 */
public class Draft17Section4WebTransportFeaturesTest {

  @Test
  public void testSection4_2_UnidirectionalStreamTypeRecognitionInApplication() {
    UnknownStreamHandlerFactory factory = new UnknownStreamHandlerFactory();

    ChannelHandler wtHandler = factory.apply(WebTransportUtils.UNI_STREAM_TYPE);
    assertNotNull(wtHandler, "Stream type 0x54 must produce WebTransportUniStreamInitializer");
    assertTrue(wtHandler instanceof WebTransportUniStreamInitializer);

    ChannelHandler unknownHandler = factory.apply(0x99L);
    assertNotNull(unknownHandler);
    assertFalse(unknownHandler instanceof WebTransportUniStreamInitializer);
  }

  @Test
  public void testSection4_2_UnidirectionalStreamDecoderInApplication() throws Exception {
    final WebTransportUniStreamHeaderDecoder decoder =
        new WebTransportUniStreamHeaderDecoder(WebTransportUtils.UNI_STREAM_TYPE);

    final ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.pipeline()).thenReturn(mock(ChannelPipeline.class));
    final QuicStreamChannel mockStream = mock(QuicStreamChannel.class);
    final QuicChannel mockParent = mock(QuicChannel.class);
    final QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);

    final DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    final DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    final DefaultAttributeMap connectAttrMap = new DefaultAttributeMap();

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);
    parentAttrMap.attr(WebTransportAttributeKeys.LOCAL_SETTINGS_MAX_STREAMS_UNI).set(100L);
    parentAttrMap.attr(WebTransportAttributeKeys.SESSION_PATH_KEY).set("/wt-path");

    when(mockParent.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    long sessionId = 12L;
    when(mockConnectStream.parent()).thenReturn(mockParent);
    when(mockConnectStream.streamId()).thenReturn(sessionId);
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(mockConnectStream.attr(any())).thenAnswer(inv -> connectAttrMap.attr(inv.getArgument(0)));
    mgr.register(mockConnectStream);

    when(mockCtx.channel()).thenReturn(mockStream);
    when(mockStream.parent()).thenReturn(mockParent);
    when(mockStream.type()).thenReturn(QuicStreamType.UNIDIRECTIONAL);
    when(mockStream.streamId()).thenReturn(7L);
    when(mockStream.closeFuture())
        .thenReturn(new DefaultChannelPromise(mockStream, ImmediateEventExecutor.INSTANCE));
    when(mockStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));

    ByteBuf in = Unpooled.buffer();
    WebTransportUtils.writeVarInt(in, sessionId);
    byte[] payloadBytes = "UniPayloadData".getBytes(StandardCharsets.UTF_8);
    in.writeBytes(payloadBytes);

    decoder.channelRead(mockCtx, in);

    assertEquals(
        Long.valueOf(sessionId),
        streamAttrMap.attr(WebTransportAttributeKeys.SESSION_ID_KEY).get());
    assertEquals(
        Long.valueOf(0x54L), streamAttrMap.attr(WebTransportAttributeKeys.STREAM_TYPE_KEY).get());

    ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
    verify(mockCtx).fireChannelRead(captor.capture());
    ByteBuf forwarded = captor.getValue();
    byte[] actualPayload = new byte[forwarded.readableBytes()];
    forwarded.readBytes(actualPayload);
    forwarded.release();
    assertArrayEquals(payloadBytes, actualPayload);
  }

  @Test
  public void testSection4_3_BidirectionalStreamDetectorHijacksWtStreamAndInstallsGuardOnHttp3() {
    WebTransportDetectorHandler detector = new WebTransportDetectorHandler();
    EmbeddedChannel wtChannel = new EmbeddedChannel(detector, new HttpServerCodec());

    ByteBuf bidiSignalBuf = Unpooled.buffer();
    WebTransportUtils.writeVarInt(bidiSignalBuf, WebTransportUtils.BI_STREAM_TYPE);
    WebTransportUtils.writeVarInt(bidiSignalBuf, 4L);
    wtChannel.writeInbound(bidiSignalBuf);

    assertNull(wtChannel.pipeline().get(HttpServerCodec.class));
    wtChannel.finishAndReleaseAll();

    // Standard HTTP/3 request stream (first varint = 0x01 HEADERS): installs Http3WtStreamFrameGuard
    final EmbeddedChannel h3Channel = new EmbeddedChannel(new WebTransportDetectorHandler());
    ByteBuf h3HeadersBuf = Unpooled.buffer();
    WebTransportUtils.writeVarInt(h3HeadersBuf, 0x01L);
    WebTransportUtils.writeVarInt(h3HeadersBuf, 2L);
    h3HeadersBuf.writeBytes(new byte[] {0x00, 0x00});
    h3Channel.writeInbound(h3HeadersBuf);

    assertNotNull(
        h3Channel.pipeline().get(Http3WtStreamFrameGuard.class),
        "Standard HTTP/3 request stream MUST retain Http3WtStreamFrameGuard after detection");
    h3Channel.finishAndReleaseAll();
  }

  @Test
  public void testSection4_3_UnexpectedWtStreamAfterHeadersTriggersH3FrameUnexpected() {
    AtomicInteger capturedErrorCode = new AtomicInteger(-1);
    Http3WtStreamFrameGuard guard =
        Http3WtStreamFrameGuard.forRequestStream((ctx, code) -> capturedErrorCode.set(code));
    final EmbeddedChannel channel = new EmbeddedChannel(guard);

    // Valid HEADERS frame (type=0x01, len=3), then DATA frame (type=0x00, len=2)
    ByteBuf validFrames = Unpooled.buffer();
    WebTransportUtils.writeVarInt(validFrames, 0x01L);
    WebTransportUtils.writeVarInt(validFrames, 3L);
    validFrames.writeBytes(new byte[] {1, 2, 3});
    WebTransportUtils.writeVarInt(validFrames, 0x00L);
    WebTransportUtils.writeVarInt(validFrames, 2L);
    validFrames.writeBytes(new byte[] {0x41, 0x41}); // 0x41 inside DATA payload MUST NOT false-trip
    channel.writeInbound(validFrames);

    assertEquals(-1, capturedErrorCode.get(), "0x41 inside DATA payload must be allowed");
    ByteBuf passed = channel.readInbound();
    assertNotNull(passed);
    passed.release();

    // Now send WT_STREAM varint (0x41 -> 2-byte QUIC varint 0x40 0x41) with NO HTTP/3 length field
    ByteBuf wtStreamVarInt = Unpooled.buffer(2);
    WebTransportUtils.writeVarInt(wtStreamVarInt, WebTransportUtils.BI_STREAM_TYPE);
    channel.writeInbound(wtStreamVarInt);
    assertEquals(
        WebTransportProtocolSpec.H3_FRAME_UNEXPECTED,
        capturedErrorCode.get(),
        "WT_STREAM (0x41) after HEADERS MUST trigger H3_FRAME_UNEXPECTED (0x0105)");
    channel.finishAndReleaseAll();
  }

  @Test
  public void testSection4_3_FragmentedWtStreamOnControlAndPushStreamsTriggersH3FrameUnexpected() {
    // 1. Control stream (0x00) followed by SETTINGS (0x04, len=0) then fragmented 2-byte 0x41
    AtomicInteger controlError = new AtomicInteger(-1);
    EmbeddedChannel controlCh =
        new EmbeddedChannel(
            Http3WtStreamFrameGuard.forUnidirectionalStream(
                (ctx, code) -> controlError.set(code)));
    controlCh.writeInbound(Unpooled.wrappedBuffer(new byte[] {0x00, 0x04, 0x00, 0x40}));
    assertEquals(-1, controlError.get());
    controlCh.writeInbound(Unpooled.wrappedBuffer(new byte[] {0x41}));
    assertEquals(WebTransportProtocolSpec.H3_FRAME_UNEXPECTED, controlError.get());
    controlCh.finishAndReleaseAll();

    // 2. Push stream (0x01 + pushId=7) followed by 2-byte QUIC varint 0x41 (0x40, 0x41)
    AtomicInteger pushError = new AtomicInteger(-1);
    EmbeddedChannel pushCh =
        new EmbeddedChannel(
            Http3WtStreamFrameGuard.forUnidirectionalStream((ctx, code) -> pushError.set(code)));
    pushCh.writeInbound(Unpooled.wrappedBuffer(new byte[] {0x01, 0x07, 0x40, 0x41}));
    assertEquals(WebTransportProtocolSpec.H3_FRAME_UNEXPECTED, pushError.get());
    pushCh.finishAndReleaseAll();

    // 3. QPACK encoder stream (0x02) carrying raw byte 0x41 MUST bypass guard and NOT fail
    AtomicInteger qpackError = new AtomicInteger(-1);
    EmbeddedChannel qpackCh =
        new EmbeddedChannel(
            Http3WtStreamFrameGuard.forUnidirectionalStream((ctx, code) -> qpackError.set(code)));
    qpackCh.writeInbound(Unpooled.wrappedBuffer(new byte[] {0x02, 0x41, 0x41}));
    assertEquals(-1, qpackError.get());
    assertTrue(qpackCh.pipeline().get(Http3WtStreamFrameGuard.class) == null);
    qpackCh.finishAndReleaseAll();
  }

  @Test
  public void testSection4_3_DecodedUnknownFrame0x41RejectedByControlAndRequestHandlers()
      throws Exception {
    // Control stream handler defense-in-depth
    ChannelHandlerContext controlCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel controlStream = mock(QuicStreamChannel.class);
    QuicChannel quic = mock(QuicChannel.class);
    when(controlCtx.channel()).thenReturn(controlStream);
    when(controlStream.parent()).thenReturn(quic);

    Http3InboundControlStreamHandler controlHandler = new Http3InboundControlStreamHandler();
    controlHandler.channelRead(
        controlCtx, new DefaultHttp3UnknownFrame(0x41L, Unpooled.EMPTY_BUFFER));
    verify(quic).close(eq(true), eq(WebTransportProtocolSpec.H3_FRAME_UNEXPECTED), any());

    // Request stream handler defense-in-depth
    ChannelHandlerContext reqCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel reqStream = mock(QuicStreamChannel.class);
    QuicChannel reqQuic = mock(QuicChannel.class);
    when(reqCtx.channel()).thenReturn(reqStream);
    when(reqStream.parent()).thenReturn(reqQuic);

    WebTransportHeadersHandler.INSTANCE.channelRead(
        reqCtx, new DefaultHttp3UnknownFrame(0x41L, Unpooled.EMPTY_BUFFER));
    verify(reqQuic).close(eq(true), eq(WebTransportProtocolSpec.H3_FRAME_UNEXPECTED), any());
  }

  @Test
  public void testSection4_4_ResetStreamAtReliableSizeEqualsHeaderPlusApplicationOffset() {
    AtomicLong capturedHttpCode = new AtomicLong();
    AtomicLong capturedWireReliableSize = new AtomicLong();

    QuicStreamChannel localBidiChannel = mock(QuicStreamChannel.class);
    when(localBidiChannel.isLocalCreated()).thenReturn(true);
    when(localBidiChannel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    when(localBidiChannel.streamId()).thenReturn(5L);
    DefaultChannelPromise promise =
        new DefaultChannelPromise(localBidiChannel, ImmediateEventExecutor.INSTANCE);

    StreamResetTransport capturingTransport =
        (ch, appCode, httpCode, wireReliableSize) -> {
          capturedHttpCode.set(httpCode);
          capturedWireReliableSize.set(wireReliableSize);
          return promise;
        };

    // Session ID = 64 requires 2-byte varint; stream type 0x41 requires 2-byte varint -> 4-byte hdr
    long sessionId = 64L;
    int expectedBidiHeader =
        WebTransportUtils.varIntLength(0x41L) + WebTransportUtils.varIntLength(sessionId);
    assertEquals(4, expectedBidiHeader);

    DefaultNettyWebTransportStream localBidiStream =
        new DefaultNettyWebTransportStream(localBidiChannel, sessionId, null, capturingTransport);
    localBidiStream.reset(7L, 128L);

    assertEquals(WebTransportUtils.webTransportCodeToHttpCode(7L), capturedHttpCode.get());
    assertEquals(expectedBidiHeader + 128L, capturedWireReliableSize.get());

    // Peer-initiated bidirectional stream has 0 bytes of WebTransport header on our send side
    QuicStreamChannel peerBidiChannel = mock(QuicStreamChannel.class);
    when(peerBidiChannel.isLocalCreated()).thenReturn(false);
    when(peerBidiChannel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    DefaultNettyWebTransportStream peerBidiStream =
        new DefaultNettyWebTransportStream(peerBidiChannel, sessionId, null, capturingTransport);
    peerBidiStream.reset(7L, 128L);

    assertEquals(
        128L,
        capturedWireReliableSize.get(),
        "Send side of peer-initiated bidirectional stream has 0 WT header bytes");

    assertThrows(IllegalArgumentException.class, () -> peerBidiStream.reset(0L, -1L));
  }

  @Test
  public void testSection4_4_InboundResetErrorMappingAndNonApplicationErrorDelivery() {
    // 1. Valid WT_APPLICATION_ERROR boundary round-trips
    long httpZero = WebTransportUtils.webTransportCodeToHttpCode(0x00000000L);
    assertEquals(0x52e4a40fa8dbL, httpZero);
    assertEquals(0x00000000L, WebTransportUtils.httpCodeToWebTransportCode(httpZero));

    long httpMax = WebTransportUtils.webTransportCodeToHttpCode(0xffffffffL);
    assertEquals(0x52e5ac983162L, httpMax);
    assertEquals(0xffffffffL, WebTransportUtils.httpCodeToWebTransportCode(httpMax));

    // 2. Non-application HTTP/3 error codes MUST NOT be classified as WT_APPLICATION_ERROR
    long[] nonAppCodes = {
      0x0100L, // H3_NO_ERROR
      0x010cL, // H3_REQUEST_CANCELLED
      0x3994bd84L, // WT_BUFFERED_STREAM_REJECTED
      0x170d7b68L, // WT_SESSION_GONE
      0x045d4487L, // WT_FLOW_CONTROL_ERROR
      0x21L, // Reserved GREASE codepoint
      WebTransportUtils.WT_ERROR_FIRST + 30L // Reserved 0x1f * N + 0x21 inside range
    };
    for (long nonApp : nonAppCodes) {
      assertFalse(
          WebTransportUtils.isWebTransportApplicationError(nonApp),
          "Non-application HTTP/3 code 0x" + Long.toHexString(nonApp) + " MUST NOT match");
      assertFalse(WebTransportUtils.tryMapHttpToApplicationError(nonApp).isPresent());
    }

    // 3. DefaultMessageDispatcher delivers mapped code for WT_APPLICATION_ERROR and empty for H3
    QuicStreamChannel mockStreamChannel = mock(QuicStreamChannel.class);
    when(mockStreamChannel.isActive()).thenReturn(true);
    when(mockStreamChannel.type()).thenReturn(QuicStreamType.BIDIRECTIONAL);
    when(mockStreamChannel.newPromise())
        .thenReturn(new DefaultChannelPromise(mockStreamChannel, ImmediateEventExecutor.INSTANCE));
    DefaultAttributeMap attrs = new DefaultAttributeMap();
    when(mockStreamChannel.attr(any())).thenAnswer(inv -> attrs.attr(inv.getArgument(0)));

    AtomicReference<Throwable> receivedError = new AtomicReference<>();
    DefaultNettyWebTransportStream appStream =
        new DefaultNettyWebTransportStream(mockStreamChannel, 0L);
    appStream.onError(receivedError::set);
    attrs.attr(WebTransportAttributeKeys.WT_STREAM_KEY).set(appStream);

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockStreamChannel);

    // Non-application reset (H3_REQUEST_CANCELLED = 0x010c) -> empty applicationErrorCode()
    DefaultMessageDispatcher.INSTANCE.exceptionCaught(
        ctx, new QuicStreamResetException("cancelled", 0x010cL));
    assertTrue(receivedError.get() instanceof WebTransportStreamResetException);
    WebTransportStreamResetException nonAppEx =
        (WebTransportStreamResetException) receivedError.get();
    assertFalse(nonAppEx.hasApplicationErrorCode());
    assertFalse(nonAppEx.applicationErrorCode().isPresent());
    assertEquals(0x010cL, nonAppEx.rawHttp3ErrorCode());

    // Application reset (N = 404) -> applicationErrorCode() == 404 unchanged
    long mapped404 = WebTransportUtils.webTransportCodeToHttpCode(404L);
    DefaultMessageDispatcher.INSTANCE.exceptionCaught(
        ctx, new QuicStreamResetException("app_reset", mapped404));
    WebTransportStreamResetException appEx =
        (WebTransportStreamResetException) receivedError.get();
    assertTrue(appEx.hasApplicationErrorCode());
    assertEquals(404L, appEx.applicationErrorCode().getAsLong());
  }

  @Test
  public void testSection4_5_DatagramDecoderQuarterStreamIdInApplication() {
    EmbeddedChannel channel = new EmbeddedChannel(WebTransportDatagramDecoder.INSTANCE);

    ByteBuf datagramBuf = Unpooled.buffer();
    WebTransportUtils.writeVarInt(datagramBuf, 3L);
    byte[] dgramData = "DatagramContent".getBytes(StandardCharsets.UTF_8);
    datagramBuf.writeBytes(dgramData);

    channel.writeInbound(datagramBuf);

    WebTransportDatagramFrame frame = channel.readInbound();
    assertNotNull(frame);
    assertEquals(12L, frame.sessionId());

    byte[] payload = new byte[frame.content().readableBytes()];
    frame.content().readBytes(payload);
    frame.release();
    assertArrayEquals(dgramData, payload);
    channel.finishAndReleaseAll();
  }

  @Test
  public void testSection4_7_SessionDrainCapsuleInApplication() throws Exception {
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    QuicStreamChannel mockConnectStream = mock(QuicStreamChannel.class);
    QuicChannel mockQuic = mock(QuicChannel.class);

    DefaultAttributeMap parentAttrMap = new DefaultAttributeMap();
    when(mockCtx.channel()).thenReturn(mockConnectStream);
    when(mockConnectStream.parent()).thenReturn(mockQuic);
    when(mockConnectStream.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(mockConnectStream.isOpen()).thenReturn(true);
    when(mockQuic.attr(any())).thenAnswer(inv -> parentAttrMap.attr(inv.getArgument(0)));

    WebTransportSessionManager mgr = new WebTransportSessionManager();
    parentAttrMap.attr(WebTransportAttributeKeys.WT_SESSION_MGR).set(mgr);

    DefaultAttributeMap streamAttrMap = new DefaultAttributeMap();
    when(mockConnectStream.attr(any())).thenAnswer(inv -> streamAttrMap.attr(inv.getArgument(0)));
    when(mockConnectStream.hasAttr(any()))
        .thenAnswer(inv -> streamAttrMap.hasAttr(inv.getArgument(0)));

    mgr.register(mockConnectStream);
    WebTransportSession session = mgr.get(0L);
    assertNotNull(session);
    assertFalse(session.isDraining());

    WebTransportCapsuleHandler.INSTANCE.channelRead(
        mockCtx, new WebTransportCapsule(0L, 0x78aeL, Unpooled.EMPTY_BUFFER));
    assertTrue(session.isDraining());

    WebTransportCapsule invalidDrainCapsule =
        new WebTransportCapsule(0L, 0x78aeL, Unpooled.wrappedBuffer(new byte[] {0x01}));
    WebTransportCapsuleHandler.INSTANCE.channelRead(mockCtx, invalidDrainCapsule);
    verify(mockConnectStream).shutdown(eq(0x010e), any());
  }

  @Test
  public void testSection4_8_KeyingMaterialExporterContextSerializationInApplication() {
    long sessionId = 16L;
    String appLabel = "test-label";
    byte[] appContext = "MyAppContext".getBytes(StandardCharsets.UTF_8);

    byte[] serialized =
        WebTransportKeyExporter.serializeExporterContext(sessionId, appLabel, appContext);

    ByteBuf buf = Unpooled.wrappedBuffer(serialized);
    try {
      assertEquals(sessionId, buf.readLong());
      int labelLen = buf.readUnsignedByte();
      byte[] decodedLabel = new byte[labelLen];
      buf.readBytes(decodedLabel);
      int contextLen = buf.readUnsignedByte();
      byte[] decodedContext = new byte[contextLen];
      buf.readBytes(decodedContext);

      assertEquals(appLabel, new String(decodedLabel, StandardCharsets.US_ASCII));
      assertArrayEquals(appContext, decodedContext);
    } finally {
      buf.release();
    }
  }
}

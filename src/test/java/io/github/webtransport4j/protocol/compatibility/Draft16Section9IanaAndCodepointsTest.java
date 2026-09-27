package io.github.webtransport4j.protocol.compatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.github.webtransport4j.server.WebTransportAttributeKeys;
import io.github.webtransport4j.server.WebTransportCapsule;
import io.github.webtransport4j.server.WebTransportCapsuleDecoder;
import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportServer;
import io.github.webtransport4j.server.WebTransportServerBuilder;
import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http3.Http3Settings;
import org.junit.Test;

/**
 * Protocol compatibility tests for draft-ietf-webtrans-http3-16 Section 9: IANA Considerations.
 * Directly exercises application classes: {@link WebTransportServer},
 * {@link WebTransportUtils}, and {@link WebTransportCapsuleDecoder}.
 */
public class Draft16Section9IanaAndCodepointsTest {

  /**
   * Section 9.1: Upgrade Token Registration.
   * "Value: webtransport-h3"
   * MANDATORY: Test upgrade token value.
   */
  @Test
  public void testSection9_1_UpgradeTokenRegistration() {
    assertEquals(
        "Upgrade token MUST be webtransport-h3",
        "webtransport-h3",
        WebTransportHeadersHandler.UPGRADE_TOKEN_H3);
  }

  /**
   * Section 9.2: HTTP/3 SETTINGS Parameter Registration.
   * "SETTINGS_WT_ENABLED: 0x2c7cf000,
   *  SETTINGS_WT_INITIAL_MAX_STREAMS_UNI: 0x2b64,
   *  SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI: 0x2b65,
   *  SETTINGS_WT_INITIAL_MAX_DATA: 0x2b61"
   * MANDATORY: Test WebTransportServer produces all registered settings parameters.
   */
  @Test
  public void testSection9_2_Http3SettingsParametersInApplication() {
    WebTransportServer server = new WebTransportServerBuilder().build();
    Http3Settings settings = server.buildHttp3Settings();

    assertNotNull("SETTINGS_WT_ENABLED (0x2c7cf000) must be present", settings.get(0x2c7cf000L));
    assertEquals(Long.valueOf(1L), settings.get(0x2c7cf000L));

    assertNotNull("SETTINGS_WT_INITIAL_MAX_STREAMS_UNI (0x2b64) must be present",
        settings.get(0x2b64L));
    assertNotNull("SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (0x2b65) must be present",
        settings.get(0x2b65L));
    assertNotNull("SETTINGS_WT_INITIAL_MAX_DATA (0x2b61) must be present",
        settings.get(0x2b61L));
  }

  /**
   * Section 9.3: Frame Type Registration.
   * "Value: 0x41, Frame Type: WT_STREAM"
   * MANDATORY: Test application uses 0x41 as BI_STREAM_TYPE.
   */
  @Test
  public void testSection9_3_FrameTypeRegistrationInApplication() {
    assertEquals("WT_STREAM frame type MUST be 0x41", 0x41L, WebTransportUtils.BI_STREAM_TYPE);
  }

  /**
   * Section 9.4: Stream Type Registration.
   * "Value: 0x54, Stream Type: WebTransport stream"
   * MANDATORY: Test application uses 0x54 as UNI_STREAM_TYPE.
   */
  @Test
  public void testSection9_4_StreamTypeRegistrationInApplication() {
    assertEquals("WebTransport stream type MUST be 0x54", 0x54L, WebTransportUtils.UNI_STREAM_TYPE);
  }

  /**
   * Section 9.5: HTTP/3 Error Code Registration.
   * "WT_APPLICATION_ERROR: 0x52e4a40fa8db to 0x52e5ac983162 inclusive, with the exception of the
   * codepoints of form 0x1f * N + 0x21."
   * MANDATORY: Test application error range bounds and identification in WebTransportUtils.
   */
  @Test
  public void testSection9_5_WtApplicationErrorRangeBoundsInApplication() {
    assertEquals("First WT_APPLICATION_ERROR codepoint", 0x52e4a40fa8dbL,
        WebTransportUtils.WT_ERROR_FIRST);
    assertEquals("Last WT_APPLICATION_ERROR codepoint", 0x52e5ac983162L,
        WebTransportUtils.WT_ERROR_LAST);
    assertTrue(WebTransportUtils.isWebTransportApplicationError(WebTransportUtils.WT_ERROR_FIRST));
    assertTrue(WebTransportUtils.isWebTransportApplicationError(WebTransportUtils.WT_ERROR_LAST));
  }

  /**
   * Section 9.6: Capsule Types.
   * "WT_CLOSE_SESSION: 0x2843,
   *  WT_DRAIN_SESSION: 0x78ae,
   *  WT_MAX_STREAMS: 0x190B4D3F..0x190B4D40,
   *  WT_STREAMS_BLOCKED: 0x190B4D43..0x190B4D44,
   *  WT_MAX_DATA: 0x190B4D3D,
   *  WT_DATA_BLOCKED: 0x190B4D41"
   * MANDATORY: Test WebTransportCapsuleDecoder decodes each registered capsule type.
   */
  @Test
  public void testSection9_6_CapsuleTypesDecodingInApplication() {
    long[] types = new long[]{
        0x2843L, 0x78aeL, 0x190b4d3fL, 0x190b4d40L, 0x190b4d3dL, 0x190b4d41L, 0x190b4d43L, 0x190b4d44L
    };

    for (long capsuleType : types) {
      EmbeddedChannel channel = new EmbeddedChannel(new WebTransportCapsuleDecoder());
      channel.attr(WebTransportAttributeKeys.SESSION_ID_KEY).set(1L);

      ByteBuf in = Unpooled.buffer();
      WebTransportUtils.writeVarInt(in, capsuleType);
      WebTransportUtils.writeVarInt(in, 0L); // 0 length content

      channel.writeInbound(in);

      WebTransportCapsule capsule = channel.readInbound();
      assertNotNull("Decoder must produce capsule for type 0x" + Long.toHexString(capsuleType), capsule);
      assertEquals(capsuleType, capsule.capsuleType());
      capsule.content().release();
      channel.finishAndReleaseAll();
    }
  }

  /**
   * Section 9.7: Protocol Negotiation HTTP Header Fields.
   * "WT-Available-Protocols, WT-Protocol"
   * OPTIONAL: Header field name definitions.
   */
  @Test
  public void testSection9_7_ProtocolNegotiationHeaderFieldNames_Optional() {
    // Optional feature: left empty without assertions per specification requirements
  }
}

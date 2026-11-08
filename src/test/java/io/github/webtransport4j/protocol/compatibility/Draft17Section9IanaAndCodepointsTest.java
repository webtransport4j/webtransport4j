package io.github.webtransport4j.protocol.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.server.WebTransportHeadersHandler;
import io.github.webtransport4j.server.WebTransportProtocolSpec;
import io.github.webtransport4j.server.WebTransportUtils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests verifying {@code draft-ietf-webtrans-http3-17} Section 9: IANA Considerations and Wire
 * Codepoints.
 */
public class Draft17Section9IanaAndCodepointsTest {

  @Test
  public void testSection9_1_UpgradeTokenRegistration() {
    assertEquals("webtransport-h3", WebTransportProtocolSpec.UPGRADE_TOKEN_H3);
    assertEquals(WebTransportProtocolSpec.UPGRADE_TOKEN_H3, WebTransportHeadersHandler.UPGRADE_TOKEN_H3);
  }

  @Test
  public void testSection9_2_Http3SettingsRegistrations() {
    assertEquals(
        0x20b8933bL,
        WebTransportProtocolSpec.SETTING_WT_ENABLED,
        "Draft-17 Section 9.2 updates SETTINGS_WT_ENABLED from 0x2c7cf000 to 0x20b8933b");
    assertEquals(0x2b61L, WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_DATA);
    assertEquals(0x2b64L, WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_UNI);
    assertEquals(0x2b65L, WebTransportProtocolSpec.SETTING_WT_INITIAL_MAX_STREAMS_BIDI);
  }

  @Test
  public void testSection9_3_And_9_4_FrameAndStreamTypeRegistrations() {
    assertEquals(
        0x41L, WebTransportProtocolSpec.WT_STREAM_TYPE_BIDI, "WT_STREAM frame type MUST be 0x41");
    assertEquals(
        0x54L,
        WebTransportProtocolSpec.WT_STREAM_TYPE_UNI,
        "WebTransport unidirectional stream MUST be 0x54");
    assertEquals(WebTransportProtocolSpec.WT_STREAM_TYPE_BIDI, WebTransportUtils.BI_STREAM_TYPE);
    assertEquals(WebTransportProtocolSpec.WT_STREAM_TYPE_UNI, WebTransportUtils.UNI_STREAM_TYPE);
    assertEquals(0x00L, WebTransportProtocolSpec.H3_STREAM_TYPE_CONTROL);
    assertEquals(0x01L, WebTransportProtocolSpec.H3_STREAM_TYPE_PUSH);
  }

  @Test
  public void testSection9_5_Http3ErrorCodesAndApplicationErrorRange() {
    assertEquals(0x3994bd84, WebTransportProtocolSpec.WT_BUFFERED_STREAM_REJECTED);
    assertEquals(0x170d7b68, WebTransportProtocolSpec.WT_SESSION_GONE);
    assertEquals(0x045d4487, WebTransportProtocolSpec.WT_FLOW_CONTROL_ERROR);
    assertEquals(0x0817b3dd, WebTransportProtocolSpec.WT_ALPN_ERROR);
    assertEquals(0x212c0d48, WebTransportProtocolSpec.WT_REQUIREMENTS_NOT_MET);
    assertEquals(0x0105, WebTransportProtocolSpec.H3_FRAME_UNEXPECTED);
    assertEquals(0x0109, WebTransportProtocolSpec.H3_SETTINGS_ERROR);
    assertEquals(0x010e, WebTransportProtocolSpec.H3_MESSAGE_ERROR);

    assertEquals(0x52e4a40fa8dbL, WebTransportProtocolSpec.WT_APPLICATION_ERROR_FIRST);
    assertEquals(0x52e5ac983162L, WebTransportProtocolSpec.WT_APPLICATION_ERROR_LAST);
    assertEquals(
        WebTransportProtocolSpec.WT_APPLICATION_ERROR_FIRST, WebTransportUtils.WT_ERROR_FIRST);
    assertEquals(
        WebTransportProtocolSpec.WT_APPLICATION_ERROR_LAST, WebTransportUtils.WT_ERROR_LAST);

    // Endpoint boundaries of WT_APPLICATION_ERROR (0x00000000 .. 0xffffffff)
    assertEquals(WebTransportUtils.WT_ERROR_FIRST, WebTransportUtils.webTransportCodeToHttpCode(0L));
    assertEquals(
        WebTransportUtils.WT_ERROR_LAST, WebTransportUtils.webTransportCodeToHttpCode(0xffffffffL));
    assertEquals(0L, WebTransportUtils.httpCodeToWebTransportCode(WebTransportUtils.WT_ERROR_FIRST));
    assertEquals(
        0xffffffffL, WebTransportUtils.httpCodeToWebTransportCode(WebTransportUtils.WT_ERROR_LAST));

    // Non-application HTTP/3 codes and reserved 0x1f*N + 0x21 GREASE codepoints MUST be rejected
    assertFalse(WebTransportUtils.isWebTransportApplicationError(0x0100L));
    assertFalse(WebTransportUtils.isWebTransportApplicationError(0x010cL));
    assertFalse(WebTransportUtils.isWebTransportApplicationError(0x21L));
    long greaseInsideRange = WebTransportUtils.WT_ERROR_FIRST + 30L; // (h - 0x21) % 31 == 0
    assertEquals(0L, (greaseInsideRange - 0x21L) % 31L);
    assertFalse(WebTransportUtils.isWebTransportApplicationError(greaseInsideRange));
    assertThrows(
        IllegalArgumentException.class,
        () -> WebTransportUtils.httpCodeToWebTransportCode(0x010cL));
    assertThrows(
        IllegalArgumentException.class,
        () -> WebTransportUtils.httpCodeToWebTransportCode(greaseInsideRange));
  }

  @Test
  public void testSection9_6_And_9_7_HeaderFieldsAndCapsuleTypes() {
    assertEquals(
        "wt-available-protocols", WebTransportProtocolSpec.HEADER_WT_AVAILABLE_PROTOCOLS);
    assertEquals("wt-protocol", WebTransportProtocolSpec.HEADER_WT_PROTOCOL);
    assertEquals(
        WebTransportProtocolSpec.HEADER_WT_AVAILABLE_PROTOCOLS,
        WebTransportHeadersHandler.HEADER_WT_AVAILABLE_PROTOCOLS);
    assertEquals(
        WebTransportProtocolSpec.HEADER_WT_PROTOCOL, WebTransportHeadersHandler.HEADER_WT_PROTOCOL);

    // Verify Section 9.7 capsule type codepoints match Draft-17 / draft-ietf-webtrans-overview
    assertEquals(0x2843L, WebTransportProtocolSpec.CAPSULE_CLOSE_WEBTRANSPORT_SESSION);
    assertEquals(0x78aeL, WebTransportProtocolSpec.CAPSULE_DRAIN_WEBTRANSPORT_SESSION);
    assertEquals(0x190b4d3dL, WebTransportProtocolSpec.CAPSULE_WT_MAX_DATA);
    assertEquals(0x190b4d3eL, WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAM_DATA);
    assertEquals(0x190b4d3fL, WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAMS_BIDI);
    assertEquals(0x190b4d40L, WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAMS_UNI);
    assertEquals(0x190b4d41L, WebTransportProtocolSpec.CAPSULE_WT_DATA_BLOCKED);
    assertEquals(0x190b4d42L, WebTransportProtocolSpec.CAPSULE_WT_STREAM_DATA_BLOCKED);
    assertEquals(0x190b4d43L, WebTransportProtocolSpec.CAPSULE_WT_STREAMS_BLOCKED_BIDI);
    assertEquals(0x190b4d44L, WebTransportProtocolSpec.CAPSULE_WT_STREAMS_BLOCKED_UNI);

    assertEquals(
        WebTransportProtocolSpec.CAPSULE_WT_MAX_DATA + 1L,
        WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAM_DATA);
    assertEquals(
        WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAM_DATA + 1L,
        WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAMS_BIDI);
    assertEquals(
        WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAMS_BIDI + 1L,
        WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAMS_UNI);
    assertEquals(
        WebTransportProtocolSpec.CAPSULE_WT_MAX_STREAMS_UNI + 1L,
        WebTransportProtocolSpec.CAPSULE_WT_DATA_BLOCKED);
    assertEquals(
        WebTransportProtocolSpec.CAPSULE_WT_DATA_BLOCKED + 1L,
        WebTransportProtocolSpec.CAPSULE_WT_STREAM_DATA_BLOCKED);
    assertEquals(
        WebTransportProtocolSpec.CAPSULE_WT_STREAM_DATA_BLOCKED + 1L,
        WebTransportProtocolSpec.CAPSULE_WT_STREAMS_BLOCKED_BIDI);
    assertEquals(
        WebTransportProtocolSpec.CAPSULE_WT_STREAMS_BLOCKED_BIDI + 1L,
        WebTransportProtocolSpec.CAPSULE_WT_STREAMS_BLOCKED_UNI);
  }
}

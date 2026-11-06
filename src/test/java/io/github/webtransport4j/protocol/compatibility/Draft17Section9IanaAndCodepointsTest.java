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
    assertEquals("webtransport-h3", WebTransportHeadersHandler.UPGRADE_TOKEN_H3);
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
    assertEquals(0x41L, WebTransportUtils.BI_STREAM_TYPE, "WT_STREAM frame type MUST be 0x41");
    assertEquals(
        0x54L, WebTransportUtils.UNI_STREAM_TYPE, "WebTransport unidirectional stream MUST be 0x54");
    assertEquals(0x00L, WebTransportProtocolSpec.H3_STREAM_TYPE_CONTROL);
    assertEquals(0x01L, WebTransportProtocolSpec.H3_STREAM_TYPE_PUSH);
  }

  @Test
  public void testSection9_5_Http3ErrorCodesAndApplicationErrorRange() {
    assertEquals(0x3994bd84, WebTransportUtils.WT_BUFFERED_STREAM_REJECTED);
    assertEquals(0x170d7b68, WebTransportUtils.WT_SESSION_GONE);
    assertEquals(0x045d4487, WebTransportUtils.WT_FLOW_CONTROL_ERROR);
    assertEquals(0x0817b3dd, WebTransportUtils.WT_ALPN_ERROR);
    assertEquals(0x212c0d48, WebTransportUtils.WT_REQUIREMENTS_NOT_MET);
    assertEquals(0x0105, WebTransportProtocolSpec.H3_FRAME_UNEXPECTED);
    assertEquals(0x0109, WebTransportProtocolSpec.H3_SETTINGS_ERROR);

    assertEquals(0x52e4a40fa8dbL, WebTransportUtils.WT_ERROR_FIRST);
    assertEquals(0x52e5ac983162L, WebTransportUtils.WT_ERROR_LAST);

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
    assertEquals("wt-available-protocols", WebTransportHeadersHandler.HEADER_WT_AVAILABLE_PROTOCOLS);
    assertEquals("wt-protocol", WebTransportHeadersHandler.HEADER_WT_PROTOCOL);

    // Verify Section 9.7 capsule type codepoints match Draft-17 / draft-ietf-webtrans-overview
    long closeWebTransportSession = 0x2843L;
    long drainWebTransportSession = 0x78aeL;
    final long wtMaxData = 0x190b4d3dL;
    final long wtMaxStreamData = 0x190b4d3eL;
    final long wtMaxStreamsBidi = 0x190b4d3fL;
    final long wtMaxStreamsUni = 0x190b4d40L;
    final long wtDataBlocked = 0x190b4d41L;
    final long wtStreamDataBlocked = 0x190b4d42L;
    final long wtStreamsBlockedBidi = 0x190b4d43L;
    final long wtStreamsBlockedUni = 0x190b4d44L;

    assertTrue(closeWebTransportSession == 0x2843L && drainWebTransportSession == 0x78aeL);
    assertEquals(wtMaxData + 1L, wtMaxStreamData);
    assertEquals(wtMaxStreamData + 1L, wtMaxStreamsBidi);
    assertEquals(wtMaxStreamsBidi + 1L, wtMaxStreamsUni);
    assertEquals(wtMaxStreamsUni + 1L, wtDataBlocked);
    assertEquals(wtDataBlocked + 1L, wtStreamDataBlocked);
    assertEquals(wtStreamDataBlocked + 1L, wtStreamsBlockedBidi);
    assertEquals(wtStreamsBlockedBidi + 1L, wtStreamsBlockedUni);
  }
}

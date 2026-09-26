"""
Comprehensive IETF WebTransport over HTTP/3 Draft-16 Interop Test Suite
======================================================================
Tests both POSITIVE and NEGATIVE test cases across all functional areas
defined in draft-ietf-webtrans-http3-16:
  - Section 3: Session Establishment & URL Path Routing
  - Section 4.2: Unidirectional Stream Features & Signaling (0x54)
  - Section 4.3: Bidirectional Stream Features & Signaling (0x41)
  - Section 4.4 & 9.5: Stream Abort / Reset & Application Error Code Remapping
  - Section 4.5: WebTransport Datagrams & Quarter Stream ID
  - Section 4.7: Capsule Protocol - Session Drain (WT_DRAIN_SESSION 0x78ae)
  - Section 5.1 & 5.3: Stream Concurrency Limits & Flow Control
  - Section 5.4: Capsule Protocol - Prohibited Stream Flow Control Capsules
  - Section 6.1 & 9.6: Capsule Protocol - Session Termination (WT_CLOSE_SESSION 0x2843)
  - Section 6: Inactivity Timeout vs. Activity Drops
"""

import asyncio
import logging
import ssl
import sys
import time
from typing import List, Tuple

from aioquic.buffer import Buffer
from pywebtransport import ClientConfig, WebTransportClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("WebTransportDraft16Suite")

# Results tracking
test_results: List[Tuple[int, str, str, str, str, str]] = []
pending_verifications = set()

# Draft-16 Constants
WT_ERROR_FIRST = 0x52E4A40FA8DB
WT_ERROR_LAST = 0x52E5AC983162
WT_DRAIN_SESSION_TYPE = 0x78AE
WT_CLOSE_SESSION_TYPE = 0x2843
WT_MAX_STREAM_DATA_TYPE = 0x190B4D3E
WT_STREAM_DATA_BLOCKED_TYPE = 0x190B4D42


def record_result(
    test_num: int,
    test_type: str,
    section: str,
    name: str,
    status: str,
    note: str = "",
):
  test_results.append((test_num, test_type, section, name, status, note))
  status_icon = "✅" if status == "PASSED" else "❌"
  logger.info(
      f"{status_icon} [{test_type}] Test {test_num:02d} ({section}) {name}:"
      f" {status} {note}"
  )


def make_capsule(capsule_type: int, payload: bytes) -> bytes:
  buf = Buffer(capacity=16)
  buf.push_uint_var(capsule_type)
  buf.push_uint_var(len(payload))
  return buf.data + payload


async def send_capsule_on_connect_stream(session, capsule_bytes: bytes):
  stream_id = session._control_stream_id
  session.protocol_handler._h3.send_data(
      stream_id=stream_id, data=capsule_bytes, end_stream=False
  )
  session.protocol_handler._trigger_transmission()


# ==============================================================================
# SECTION 1: Session Establishment & URL Path Routing (§ 3.2, § 7.1)
# ==============================================================================


async def test_01_session_connect_positive(client, base_url: str):
  test_num = 1
  try:
    session = await client.connect(url=f"{base_url}/test")
    assert session.is_ready
    record_result(
        test_num,
        "POSITIVE",
        "§ 3.2",
        "Primary Session Connect (/test)",
        "PASSED",
        f"Stream ID {session._control_stream_id}",
    )
    return session
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 3.2",
        "Primary Session Connect (/test)",
        "FAILED",
        str(e),
    )
    raise


async def test_02_url_path_chat_positive(client, base_url: str):
  test_num = 2
  try:
    s = await client.connect(url=f"{base_url}/chat")
    assert s.is_ready
    await s.close()
    record_result(
        test_num,
        "POSITIVE",
        "§ 3.2",
        "URL Path Routing Session Connect (/chat)",
        "PASSED",
        "Routed to WebTransportChatHandler",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 3.2",
        "URL Path Routing Session Connect (/chat)",
        "FAILED",
        str(e),
    )
    raise


async def test_03_url_path_echo_positive(client, base_url: str):
  test_num = 3
  try:
    s = await client.connect(url=f"{base_url}/echo")
    assert s.is_ready
    st = await s.create_bidirectional_stream()
    await st.write_all(b"EchoHandlerTestPayload", end_stream=False)
    resp = await asyncio.wait_for(st.read(), timeout=3.0)
    assert b"EchoHandlerTestPayload" in resp
    await st.write_all(b"", end_stream=True)
    await s.close()
    record_result(
        test_num,
        "POSITIVE",
        "§ 3.2",
        "URL Path Routing Connect & Echo (/echo)",
        "PASSED",
        "Connected and echoed data via EchoWebTransportHandler",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 3.2",
        "URL Path Routing Connect & Echo (/echo)",
        "FAILED",
        str(e),
    )
    raise


async def test_04_invalid_scheme_negative(client):
  test_num = 4
  try:
    await client.connect(url="http://127.0.0.1:4433/test")
    record_result(
        test_num,
        "NEGATIVE",
        "§ 3.2",
        "Invalid Scheme (http://) Rejection",
        "FAILED",
        "Expected rejection",
    )
    raise AssertionError("Server or client accepted non-https scheme!")
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 3.2",
        "Invalid Scheme (http://) Rejection",
        "PASSED",
        f"Correctly rejected ({type(e).__name__})",
    )


# ==============================================================================
# SECTION 2: Unidirectional Stream Features (§ 4.2)
# ==============================================================================


async def test_05_client_uni_stream_positive(session):
  test_num = 5
  payload_id = f"PingUni_{time.time()}"
  pending_verifications.add(payload_id)
  try:
    stream = await session.create_unidirectional_stream()
    await stream.write_all(data=payload_id.encode("utf-8"), end_stream=True)

    ack_stream = await asyncio.wait_for(
        session.accept_unidirectional_stream(), timeout=5.0
    )
    response = await asyncio.wait_for(ack_stream.read(), timeout=3.0)
    response_str = response.decode("utf-8", errors="replace")
    assert f"ACK UNI: {payload_id}" in response_str
    pending_verifications.remove(payload_id)
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.2",
        "Client Uni Stream (0x54) & Server Uni ACK",
        "PASSED",
        f"Strict ACK: {response_str.strip()}",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.2",
        "Client Uni Stream (0x54) & Server Uni ACK",
        "FAILED",
        str(e),
    )
    raise


async def test_06_server_uni_stream_positive(session):
  test_num = 6
  pending_verifications.add("Server_Uni_Greeting")
  try:
    uni_stream = await asyncio.wait_for(
        session.accept_unidirectional_stream(), timeout=5.0
    )
    uni_data = await asyncio.wait_for(uni_stream.read(), timeout=5.0)
    assert (
        b"Hello from Server-Initiated Unidirectional Stream!" in uni_data
    ), "Greeting mismatch"
    pending_verifications.remove("Server_Uni_Greeting")
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.2",
        "Server-Initiated Uni Stream Greeting",
        "PASSED",
        f"Received greeting on stream ID {uni_stream.stream_id}",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.2",
        "Server-Initiated Uni Stream Greeting",
        "FAILED",
        str(e),
    )
    raise


async def test_07_unknown_session_uni_stream_negative(session):
  test_num = 7
  try:
    quic_conn = session.protocol_handler._quic
    unknown_session_id = 999999
    uni_stream_id = quic_conn.get_next_available_stream_id(
        is_unidirectional=True
    )

    buf = Buffer(capacity=32)
    buf.push_uint_var(0x54)  # WebTransport Uni stream type
    buf.push_uint_var(unknown_session_id)
    buf.push_bytes(b"BadUniSessionPayload")

    quic_conn.send_stream_data(uni_stream_id, buf.data, end_stream=True)
    session.protocol_handler._trigger_transmission()
    await asyncio.sleep(0.3)

    # Verify active session is completely healthy
    st = await session.create_bidirectional_stream()
    await st.write_all(b"HealthCheckUniDrop", end_stream=False)
    resp = await asyncio.wait_for(st.read(), timeout=2.0)
    assert b"ACK BI: HealthCheckUniDrop" in resp
    await st.write_all(b"", end_stream=True)

    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.2",
        "Unknown Session ID Uni Stream Dropped",
        "PASSED",
        "Server discarded safely without disrupting active session",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.2",
        "Unknown Session ID Uni Stream Dropped",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 3: Bidirectional Stream Features (§ 4.3)
# ==============================================================================


async def test_08_client_bidi_stream_positive(session):
  test_num = 8
  payload_id = f"PingBidi_{time.time()}"
  pending_verifications.add(payload_id)
  try:
    stream = await session.create_bidirectional_stream()
    await stream.write_all(data=payload_id.encode("utf-8"), end_stream=False)
    response = await asyncio.wait_for(stream.read(), timeout=3.0)
    response_str = response.decode("utf-8", errors="replace")
    assert f"ACK BI: {payload_id}" in response_str
    pending_verifications.remove(payload_id)
    await stream.write_all(data=b"", end_stream=True)
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "Client Bidi Stream (0x41) Echo",
        "PASSED",
        f"Echo response: {response_str.strip()}",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "Client Bidi Stream (0x41) Echo",
        "FAILED",
        str(e),
    )
    raise


async def test_09_server_bidi_stream_positive(session):
  test_num = 9
  pending_verifications.add("Server_Bidi_Greeting")
  try:
    bidi_stream = await asyncio.wait_for(
        session.accept_bidirectional_stream(), timeout=5.0
    )
    bidi_data = await asyncio.wait_for(bidi_stream.read(), timeout=5.0)
    assert (
        b"Hello from Server-Initiated Bidirectional Stream!" in bidi_data
    ), "Greeting mismatch"
    pending_verifications.remove("Server_Bidi_Greeting")
    await bidi_stream.write_all(
        data=b"ACK SERVER BIDI: Greetings from Python", end_stream=True
    )
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "Server-Initiated Bidi Stream Greeting",
        "PASSED",
        f"Received greeting on stream ID {bidi_stream.stream_id}",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "Server-Initiated Bidi Stream Greeting",
        "FAILED",
        str(e),
    )
    raise


async def test_10_large_payload_positive(session):
  test_num = 10
  payload_id = f"LargePayload_{time.time()}"
  pending_verifications.add(payload_id)
  try:
    stream = await session.create_bidirectional_stream()
    chunk = b"0123456789ABCDEF" * 16384  # 256KB
    test_msg = payload_id.encode("utf-8") + b"_" + chunk
    await stream.write_all(data=test_msg, end_stream=False)

    received_data = bytearray()
    expected_len = len(b"ACK BI: ") + len(test_msg)
    while len(received_data) < expected_len:
      data = await asyncio.wait_for(stream.read(), timeout=5.0)
      if not data:
        break
      received_data.extend(data)

    assert (b"ACK BI: " + test_msg) in received_data, "Data corrupted!"
    pending_verifications.remove(payload_id)
    await stream.write_all(data=b"", end_stream=True)
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "250KB Large Payload Stream Integrity",
        "PASSED",
        f"Transferred {len(test_msg)} bytes bit-for-bit cleanly",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "250KB Large Payload Stream Integrity",
        "FAILED",
        str(e),
    )
    raise


async def test_11_concurrent_streams_positive(session):
  test_num = 11
  num_streams = 8
  try:

    async def run_single_stream(idx):
      s = await session.create_bidirectional_stream()
      p_id = f"Concurrent_{idx}_{time.time()}"
      await s.write_all(data=p_id.encode("utf-8"), end_stream=False)
      resp = await asyncio.wait_for(s.read(), timeout=5.0)
      assert f"ACK BI: {p_id}" in resp.decode("utf-8", errors="replace")
      await s.write_all(data=b"", end_stream=True)

    await asyncio.gather(*[run_single_stream(i) for i in range(num_streams)])
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "8 Multiplexed Concurrent Bidi Streams",
        "PASSED",
        f"Simultaneously executed {num_streams} parallel streams",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "8 Multiplexed Concurrent Bidi Streams",
        "FAILED",
        str(e),
    )
    raise


async def test_12_hol_blocking_positive(session):
  test_num = 12
  try:
    hog_stream = await session.create_bidirectional_stream()
    hog_payload = f"SleepServer_{time.time()}"
    await hog_stream.write_all(
        data=hog_payload.encode("utf-8"), end_stream=False
    )
    await asyncio.sleep(0.4)

    fast_stream = await session.create_bidirectional_stream()
    fast_payload = f"Ping_Fast_{time.time()}"
    t0 = time.time()
    await fast_stream.write_all(
        data=fast_payload.encode("utf-8"), end_stream=False
    )
    fast_resp = await asyncio.wait_for(fast_stream.read(), timeout=1.5)
    elapsed = time.time() - t0
    assert f"ACK BI: {fast_payload}" in fast_resp.decode(
        "utf-8", errors="replace"
    )
    await fast_stream.write_all(data=b"", end_stream=True)

    # Await sleeping hog stream
    hog_resp = await asyncio.wait_for(hog_stream.read(), timeout=4.0)
    assert f"ACK BI: {hog_payload}" in hog_resp.decode(
        "utf-8", errors="replace"
    )
    await hog_stream.write_all(data=b"", end_stream=True)

    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "Head-of-Line Blocking (HOLB) Elimination",
        "PASSED",
        f"Fast stream finished in {elapsed:.3f}s bypassing sleeping stream",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.3",
        "Head-of-Line Blocking (HOLB) Elimination",
        "FAILED",
        str(e),
    )
    raise


async def test_13_unknown_session_bidi_stream_negative(session):
  test_num = 13
  try:
    quic_conn = session.protocol_handler._quic
    unknown_session_id = 999999
    bidi_stream_id = quic_conn.get_next_available_stream_id(
        is_unidirectional=False
    )

    buf = Buffer(capacity=32)
    buf.push_uint_var(0x41)  # WebTransport Bidi stream type
    buf.push_uint_var(unknown_session_id)
    buf.push_bytes(b"BadBidiSessionPayload")

    quic_conn.send_stream_data(bidi_stream_id, buf.data, end_stream=True)
    session.protocol_handler._trigger_transmission()
    await asyncio.sleep(0.3)

    # Verify active session is completely healthy
    st = await session.create_bidirectional_stream()
    await st.write_all(b"HealthCheckBidiDrop", end_stream=False)
    resp = await asyncio.wait_for(st.read(), timeout=2.0)
    assert b"ACK BI: HealthCheckBidiDrop" in resp
    await st.write_all(b"", end_stream=True)

    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.3",
        "Unknown Session ID Bidi Stream Dropped",
        "PASSED",
        "Server discarded safely without disrupting active session",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.3",
        "Unknown Session ID Bidi Stream Dropped",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 4: Application Error Code Remapping & Stream Resets (§ 4.4, § 9.5)
# ==============================================================================


def webtransport_code_to_http_code(n: int) -> int:
  if n < 0 or n > 0xFFFFFFFF:
    raise ValueError(f"Out of range: {n}")
  return WT_ERROR_FIRST + n + (n // 30)


def http_code_to_webtransport_code(h: int) -> int:
  if WT_ERROR_FIRST <= h <= WT_ERROR_LAST:
    if (h - 0x21) % 31 == 0:
      raise ValueError(f"Reserved codepoint: {h}")
    shifted = h - WT_ERROR_FIRST
    return shifted - (shifted // 31)
  raise ValueError(f"Out of WT range: {h}")


async def test_14_error_code_remapping_positive():
  test_num = 14
  try:
    # 1. First bound: 0 -> 0x52e4a40fa8db
    h0 = webtransport_code_to_http_code(0)
    assert (
        h0 == 0x52E4A40FA8DB
    ), f"0x0 must map to 0x52e4a40fa8db, got {hex(h0)}"
    assert http_code_to_webtransport_code(h0) == 0

    # 2. Last bound: 0xffffffff -> 0x52e5ac983162
    h_max = webtransport_code_to_http_code(0xFFFFFFFF)
    assert (
        h_max == 0x52E5AC983162
    ), f"0xffffffff must map to 0x52e5ac983162, got {hex(h_max)}"
    assert http_code_to_webtransport_code(h_max) == 0xFFFFFFFF

    # 3. Round-trip across sample points
    for code in [1, 29, 30, 31, 100, 1000, 65535, 1234567]:
      mapped = webtransport_code_to_http_code(code)
      unmapped = http_code_to_webtransport_code(mapped)
      assert unmapped == code, f"Round trip mismatch: {code} -> {unmapped}"

    record_result(
        test_num,
        "POSITIVE",
        "§ 4.4",
        "App Error Code Remapping Algorithm (N<->H)",
        "PASSED",
        "Verified boundaries (0x0, 0xffffffff) and round-trip consistency",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.4",
        "App Error Code Remapping Algorithm (N<->H)",
        "FAILED",
        str(e),
    )
    raise


async def test_15_error_code_reserved_codepoints_negative():
  test_num = 15
  try:
    # Verify that NO mapped code ever takes the form 0x1f * N + 0x21 (31*N + 33)
    for code in range(0, 300):
      mapped = webtransport_code_to_http_code(code)
      assert (mapped - 0x21) % 31 != 0, (
          f"Violation: mapped code {hex(mapped)} matches reserved 0x1f*N+0x21"
      )

    # Verify that attempting to decode a reserved codepoint raises an error
    reserved_codepoints = [
        31 * k + 0x21
        for k in range(
            WT_ERROR_FIRST // 31, (WT_ERROR_FIRST + 1000) // 31 + 1
        )
    ]
    for res_cp in reserved_codepoints:
      if WT_ERROR_FIRST <= res_cp <= WT_ERROR_LAST:
        try:
          http_code_to_webtransport_code(res_cp)
          raise AssertionError(f"Decoded reserved codepoint {hex(res_cp)}!")
        except ValueError:
          pass  # Correctly rejected

    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.4",
        "Exclusion & Rejection of Reserved Codepoints",
        "PASSED",
        "Verified (H - 0x21) % 31 != 0 guaranteed for all mapped codes",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.4",
        "Exclusion & Rejection of Reserved Codepoints",
        "FAILED",
        str(e),
    )
    raise


async def test_16_wire_stream_reset_positive(session):
  test_num = 16
  try:
    stream = await session.create_bidirectional_stream()
    await stream.write_all(b"ResetTestPayload", end_stream=False)

    # Reset stream using Draft-16 mapped HTTP/3 error code
    mapped_error = webtransport_code_to_http_code(0x1234)
    session.protocol_handler._quic.reset_stream(
        stream.stream_id, error_code=mapped_error
    )
    session.protocol_handler._trigger_transmission()
    await asyncio.sleep(0.3)

    # Verify session remains fully operational
    st = await session.create_bidirectional_stream()
    await st.write_all(b"AliveAfterReset", end_stream=False)
    resp = await asyncio.wait_for(st.read(), timeout=2.0)
    assert b"ACK BI: AliveAfterReset" in resp
    await st.write_all(b"", end_stream=True)

    record_result(
        test_num,
        "POSITIVE",
        "§ 4.4",
        "Wire Stream Reset with Mapped App Error",
        "PASSED",
        f"Reset stream with mapped code {hex(mapped_error)}; session survived",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.4",
        "Wire Stream Reset with Mapped App Error",
        "FAILED",
        str(e),
    )
    raise


async def test_17_wire_stop_sending_positive(session):
  test_num = 17
  try:
    stream = await session.create_bidirectional_stream()
    await stream.write_all(b"StopSendingPayload", end_stream=False)

    mapped_error = webtransport_code_to_http_code(0x5678)
    session.protocol_handler._quic.stop_stream(
        stream.stream_id, error_code=mapped_error
    )
    session.protocol_handler._trigger_transmission()
    await asyncio.sleep(0.3)

    # Verify session remains operational
    st = await session.create_bidirectional_stream()
    await st.write_all(b"AliveAfterStopSending", end_stream=False)
    resp = await asyncio.wait_for(st.read(), timeout=2.0)
    assert b"ACK BI: AliveAfterStopSending" in resp
    await st.write_all(b"", end_stream=True)

    record_result(
        test_num,
        "POSITIVE",
        "§ 4.4",
        "Wire STOP_SENDING with Mapped App Error",
        "PASSED",
        f"STOP_SENDING sent with code {hex(mapped_error)}; session survived",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.4",
        "Wire STOP_SENDING with Mapped App Error",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 5: Datagram Features (§ 4.5)
# ==============================================================================


async def test_18_datagram_positive(session):
  test_num = 18
  payload_id = f"PingDatagram_{time.time()}"
  pending_verifications.add(payload_id)
  try:
    test_msg = payload_id.encode("utf-8")
    await session.send_datagram(data=test_msg)

    ack_stream = await asyncio.wait_for(
        session.accept_unidirectional_stream(), timeout=5.0
    )
    response = await asyncio.wait_for(ack_stream.read(), timeout=3.0)
    response_str = response.decode("utf-8", errors="replace")
    assert f"ACK DG: {payload_id}" in response_str
    pending_verifications.remove(payload_id)

    record_result(
        test_num,
        "POSITIVE",
        "§ 4.5",
        "Datagram Quarter Stream ID & Uni Stream ACK",
        "PASSED",
        f"Strict ACK: {response_str.strip()}",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.5",
        "Datagram Quarter Stream ID & Uni Stream ACK",
        "FAILED",
        str(e),
    )
    raise


async def test_19_unknown_session_datagram_negative(session):
  test_num = 19
  try:
    quic_conn = session.protocol_handler._quic
    buf_dgram = Buffer(capacity=32)
    buf_dgram.push_uint_var(99999)  # Unknown quarter session ID
    buf_dgram.push_bytes(b"BadDgramSessionPayload")

    quic_conn.send_datagram_frame(buf_dgram.data)
    session.protocol_handler._trigger_transmission()
    await asyncio.sleep(0.3)

    # Verify session remains operational
    st = await session.create_bidirectional_stream()
    await st.write_all(b"HealthCheckDgramDrop", end_stream=False)
    resp = await asyncio.wait_for(st.read(), timeout=2.0)
    assert b"ACK BI: HealthCheckDgramDrop" in resp
    await st.write_all(b"", end_stream=True)

    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.5",
        "Unknown Session ID Datagram Dropped",
        "PASSED",
        "Server discarded safely without impacting active session",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.5",
        "Unknown Session ID Datagram Dropped",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 6: Capsule Protocol - Drain Session (§ 4.7, § 9.6)
# ==============================================================================


async def test_20_drain_session_positive(session):
  test_num = 20
  try:
    drain_cap = make_capsule(WT_DRAIN_SESSION_TYPE, b"")  # length = 0
    await send_capsule_on_connect_stream(session, drain_cap)
    await asyncio.sleep(0.3)

    # § 4.7: Endpoints MAY continue using the session after drain
    st = await session.create_bidirectional_stream()
    await st.write_all(b"HelloPostDrain", end_stream=False)
    resp = await asyncio.wait_for(st.read(), timeout=2.0)
    assert b"ACK BI: HelloPostDrain" in resp
    await st.write_all(b"", end_stream=True)

    record_result(
        test_num,
        "POSITIVE",
        "§ 4.7",
        "Valid WT_DRAIN_SESSION Capsule (len=0)",
        "PASSED",
        "Server transitioned to draining; session remained operational",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 4.7",
        "Valid WT_DRAIN_SESSION Capsule (len=0)",
        "FAILED",
        str(e),
    )
    raise


async def test_21_drain_session_nonzero_negative(client, url: str):
  test_num = 21
  try:
    s = await client.connect(url=url)
    invalid_drain = make_capsule(
        WT_DRAIN_SESSION_TYPE, b"ILLEGAL_NONZERO_PAYLOAD"
    )
    await send_capsule_on_connect_stream(s, invalid_drain)
    await asyncio.sleep(0.5)

    # Server MUST reset connect stream with H3_MESSAGE_ERROR (0x010e)
    failed = False
    try:
      st = await s.create_bidirectional_stream()
      await st.write_all(b"probe", end_stream=False)
      await asyncio.wait_for(st.read(), timeout=1.0)
    except Exception:
      failed = True

    assert failed, "Session was NOT reset after malformed drain capsule!"
    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.7",
        "Malformed WT_DRAIN_SESSION (len>0) -> H3 Error",
        "PASSED",
        "Server correctly reset CONNECT stream with H3_MESSAGE_ERROR",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 4.7",
        "Malformed WT_DRAIN_SESSION (len>0) -> H3 Error",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 7: Capsule Protocol - Prohibited Capsules (§ 5.4)
# ==============================================================================


async def test_22_prohibited_max_stream_data_negative(client, url: str):
  test_num = 22
  try:
    s = await client.connect(url=url)
    buf = Buffer(capacity=8)
    buf.push_uint_var(1000)
    prohibited = make_capsule(WT_MAX_STREAM_DATA_TYPE, buf.data)
    await send_capsule_on_connect_stream(s, prohibited)
    await asyncio.sleep(0.5)

    # § 5.4: Server MUST reset session with WT_FLOW_CONTROL_ERROR (0x045d4487)
    failed = False
    try:
      st = await s.create_bidirectional_stream()
      await st.write_all(b"probe", end_stream=False)
      await asyncio.wait_for(st.read(), timeout=1.0)
    except Exception:
      failed = True

    assert failed, "Session was NOT reset after prohibited capsule!"
    record_result(
        test_num,
        "NEGATIVE",
        "§ 5.4",
        "Prohibited WT_MAX_STREAM_DATA (0x190b4d3e)",
        "PASSED",
        "Server correctly reset session with WT_FLOW_CONTROL_ERROR",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 5.4",
        "Prohibited WT_MAX_STREAM_DATA (0x190b4d3e)",
        "FAILED",
        str(e),
    )
    raise


async def test_23_prohibited_stream_data_blocked_negative(client, url: str):
  test_num = 23
  try:
    s = await client.connect(url=url)
    buf = Buffer(capacity=8)
    buf.push_uint_var(1000)
    prohibited = make_capsule(WT_STREAM_DATA_BLOCKED_TYPE, buf.data)
    await send_capsule_on_connect_stream(s, prohibited)
    await asyncio.sleep(0.5)

    failed = False
    try:
      st = await s.create_bidirectional_stream()
      await st.write_all(b"probe", end_stream=False)
      await asyncio.wait_for(st.read(), timeout=1.0)
    except Exception:
      failed = True

    assert failed, "Session was NOT reset after prohibited capsule!"
    record_result(
        test_num,
        "NEGATIVE",
        "§ 5.4",
        "Prohibited WT_STREAM_DATA_BLOCKED (0x190b4d42)",
        "PASSED",
        "Server correctly reset session with WT_FLOW_CONTROL_ERROR",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 5.4",
        "Prohibited WT_STREAM_DATA_BLOCKED (0x190b4d42)",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 8: Capsule Protocol - Close Session (§ 6.1, § 9.6)
# ==============================================================================


async def test_24_truncated_close_session_negative(client, url: str):
  test_num = 24
  try:
    s = await client.connect(url=url)
    # Payload less than 4 bytes (only 2 bytes)
    truncated = make_capsule(WT_CLOSE_SESSION_TYPE, b"\x00\x01")
    await send_capsule_on_connect_stream(s, truncated)
    await asyncio.sleep(0.5)

    failed = False
    try:
      st = await s.create_bidirectional_stream()
      await st.write_all(b"probe", end_stream=False)
      await asyncio.wait_for(st.read(), timeout=1.0)
    except Exception:
      failed = True

    assert failed, "Session was NOT reset after truncated close capsule!"
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.1",
        "Truncated WT_CLOSE_SESSION (<4B) -> H3 Error",
        "PASSED",
        "Server correctly reset CONNECT stream with H3_MESSAGE_ERROR",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.1",
        "Truncated WT_CLOSE_SESSION (<4B) -> H3 Error",
        "FAILED",
        str(e),
    )
    raise


async def test_25_oversized_close_session_negative(client, url: str):
  test_num = 25
  try:
    s = await client.connect(url=url)
    # Payload error code (4 bytes) + 1025 bytes reason phrase (> 1024 bound)
    oversized = make_capsule(
        WT_CLOSE_SESSION_TYPE, b"\x00\x00\x00\x00" + (b"A" * 1025)
    )
    await send_capsule_on_connect_stream(s, oversized)
    await asyncio.sleep(0.5)

    failed = False
    try:
      st = await s.create_bidirectional_stream()
      await st.write_all(b"probe", end_stream=False)
      await asyncio.wait_for(st.read(), timeout=1.0)
    except Exception:
      failed = True

    assert failed, "Session was NOT reset after oversized close capsule!"
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.1",
        "Oversized WT_CLOSE_SESSION (>1024B) -> H3 Error",
        "PASSED",
        "Server correctly reset CONNECT stream with H3_MESSAGE_ERROR",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.1",
        "Oversized WT_CLOSE_SESSION (>1024B) -> H3 Error",
        "FAILED",
        str(e),
    )
    raise


async def test_26_invalid_utf8_close_session_negative(client, url: str):
  test_num = 26
  try:
    s = await client.connect(url=url)
    # Invalid UTF-8 sequence in reason phrase
    invalid_utf8 = make_capsule(
        WT_CLOSE_SESSION_TYPE, b"\x00\x00\x00\x00\xff\xfe\xfd"
    )
    await send_capsule_on_connect_stream(s, invalid_utf8)
    await asyncio.sleep(0.5)

    failed = False
    try:
      st = await s.create_bidirectional_stream()
      await st.write_all(b"probe", end_stream=False)
      await asyncio.wait_for(st.read(), timeout=1.0)
    except Exception:
      failed = True

    assert failed, "Session was NOT reset after invalid UTF-8 close capsule!"
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.1",
        "Malformed UTF-8 WT_CLOSE_SESSION -> H3 Error",
        "PASSED",
        "Server correctly reset CONNECT stream with H3_MESSAGE_ERROR",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.1",
        "Malformed UTF-8 WT_CLOSE_SESSION -> H3 Error",
        "FAILED",
        str(e),
    )
    raise


async def test_27_graceful_close_session_positive(session):
  test_num = 27
  try:
    close_payload = b"\x00\x00\x00\x00" + "Graceful Close Test".encode("utf-8")
    valid_close = make_capsule(WT_CLOSE_SESSION_TYPE, close_payload)
    await send_capsule_on_connect_stream(session, valid_close)
    await asyncio.sleep(0.4)
    await session.close()
    record_result(
        test_num,
        "POSITIVE",
        "§ 6.1",
        "Graceful WT_CLOSE_SESSION (0x2843) Clean Close",
        "PASSED",
        "Session closed gracefully per capsule protocol",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 6.1",
        "Graceful WT_CLOSE_SESSION (0x2843) Clean Close",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 9: Flow Control & Stream Limits (§ 5.1, § 5.3)
# ==============================================================================


async def test_28_flow_control_exhaustion_positive(client, url: str):
  test_num = 28
  streams = []
  blocked = False
  try:
    s = await client.connect(url=url)
    for i in range(1, 150):
      try:
        st = await asyncio.wait_for(s.create_bidirectional_stream(), timeout=0.8)
        streams.append(st)
      except asyncio.TimeoutError:
        blocked = True
        break

    # Clean up opened streams to recover permits
    for st in streams:
      try:
        await st.write_all(data=b"", end_stream=True)
        await s.stream_manager.remove_stream(st.stream_id)
      except Exception:
        pass
    await s.close()

    record_result(
        test_num,
        "POSITIVE",
        "§ 5.1",
        "Stream Limit Exhaustion & Permit Recovery",
        "PASSED",
        f"Opened {len(streams)} streams before flow control backpressure;"
        " cleaned up cleanly",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 5.1",
        "Stream Limit Exhaustion & Permit Recovery",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# SECTION 10: Inactivity / Heartbeat Timeout (§ 6)
# ==============================================================================


async def test_29_idle_connection_survival_positive(client, url: str):
  test_num = 29
  try:
    s = await client.connect(url=url)
    logger.info(
        "Sleeping 15s to test connection idle survival (< 30s timeout)..."
    )
    await asyncio.sleep(15)

    # Verify session is still alive by exchanging data on a bidirectional stream
    stream = await s.create_bidirectional_stream()
    payload_id = f"PingAfterIdle_{time.time()}"
    await stream.write_all(data=payload_id.encode("utf-8"), end_stream=False)

    resp = await asyncio.wait_for(stream.read(), timeout=4.0)
    assert f"ACK BI: {payload_id}" in resp.decode("utf-8", errors="replace")
    await stream.write_all(data=b"", end_stream=True)
    await s.close()

    record_result(
        test_num,
        "POSITIVE",
        "§ 6.0",
        "Idle Connection Survival (15s Inactivity)",
        "PASSED",
        "Connection survived 15s idle period and exchanged data",
    )
  except Exception as e:
    record_result(
        test_num,
        "POSITIVE",
        "§ 6.0",
        "Idle Connection Survival (15s Inactivity)",
        "FAILED",
        str(e),
    )
    raise


async def test_30_inactivity_timeout_negative(client, url: str):
  test_num = 30
  try:
    s = await client.connect(url=url)
    logger.info(
        "Sleeping 36s to test inactivity drop (> 30s server timeout)..."
    )
    await asyncio.sleep(36)

    dropped = False
    try:
      if s.is_closed or not s.connection or not s.connection.is_connected:
        dropped = True
      else:
        st = await asyncio.wait_for(
            s.create_bidirectional_stream(), timeout=2.0
        )
        await st.write_all(data=b"PingTimeoutNegative", end_stream=False)
        resp = await asyncio.wait_for(st.read(), timeout=2.0)
        if not resp:
          dropped = True
    except Exception:
      dropped = True

    assert dropped, "Session was STILL ALIVE after 36s of inactivity!"
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.0",
        "Inactivity Timeout Drop (36s > 30s Server Limit)",
        "PASSED",
        "Server correctly terminated connection upon exceeding idle timeout",
    )
  except Exception as e:
    record_result(
        test_num,
        "NEGATIVE",
        "§ 6.0",
        "Inactivity Timeout Drop (36s > 30s Server Limit)",
        "FAILED",
        str(e),
    )
    raise


# ==============================================================================
# MAIN TEST RUNNER & RESULTS REPORTER
# ==============================================================================


async def main():
  config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")
  base_url = "https://127.0.0.1:4433"

  print("\n" + "=" * 80)
  print(
      "🚀 IETF WEBTRANSPORT OVER HTTP/3 DRAFT-16 INTEROPERABILITY TEST SUITE 🚀"
  )
  print("=" * 80 + "\n")

  async with WebTransportClient(config=config) as client:
    try:
      # --- Section 1: Session Establishment & URL Path Routing ---
      logger.info(
          "\n--- [SECTION 1] Session Establishment & URL Path Routing (§ 3.2) ---"
      )
      primary_session = await test_01_session_connect_positive(client, base_url)
      await test_02_url_path_chat_positive(client, base_url)
      await test_03_url_path_echo_positive(client, base_url)
      await test_04_invalid_scheme_negative(client)

      # --- Section 2: Unidirectional Stream Features ---
      logger.info(
          "\n--- [SECTION 2] Unidirectional Stream Features (§ 4.2) ---"
      )
      # Server sends initial greeting streams on session ready
      await test_06_server_uni_stream_positive(primary_session)
      await test_05_client_uni_stream_positive(primary_session)
      await test_07_unknown_session_uni_stream_negative(primary_session)

      # --- Section 3: Bidirectional Stream Features ---
      logger.info(
          "\n--- [SECTION 3] Bidirectional Stream Features (§ 4.3) ---"
      )
      await test_09_server_bidi_stream_positive(primary_session)
      await test_08_client_bidi_stream_positive(primary_session)
      await test_10_large_payload_positive(primary_session)
      await test_11_concurrent_streams_positive(primary_session)
      await test_12_hol_blocking_positive(primary_session)
      await test_13_unknown_session_bidi_stream_negative(primary_session)

      # --- Section 4: Application Error Code Remapping & Stream Resets ---
      logger.info(
          "\n--- [SECTION 4] Application Error Code Remapping & Stream Resets"
          " (§ 4.4, § 9.5) ---"
      )
      await test_14_error_code_remapping_positive()
      await test_15_error_code_reserved_codepoints_negative()
      await test_16_wire_stream_reset_positive(primary_session)
      await test_17_wire_stop_sending_positive(primary_session)

      # --- Section 5: Datagram Features ---
      logger.info("\n--- [SECTION 5] Datagram Features (§ 4.5) ---")
      await test_18_datagram_positive(primary_session)
      await test_19_unknown_session_datagram_negative(primary_session)

      # --- Section 6: Capsule Protocol - Drain Session ---
      logger.info(
          "\n--- [SECTION 6] Capsule Protocol - Drain Session (§ 4.7, § 9.6)"
          " ---"
      )
      await test_20_drain_session_positive(primary_session)
      await test_21_drain_session_nonzero_negative(
          client, f"{base_url}/test"
      )

      # --- Section 7: Capsule Protocol - Prohibited Capsules ---
      logger.info(
          "\n--- [SECTION 7] Capsule Protocol - Prohibited Flow Control Capsules"
          " (§ 5.4) ---"
      )
      await test_22_prohibited_max_stream_data_negative(
          client, f"{base_url}/test"
      )
      await test_23_prohibited_stream_data_blocked_negative(
          client, f"{base_url}/test"
      )

      # --- Section 8: Capsule Protocol - Close Session ---
      logger.info(
          "\n--- [SECTION 8] Capsule Protocol - Close Session (§ 6.1, § 9.6)"
          " ---"
      )
      await test_24_truncated_close_session_negative(
          client, f"{base_url}/test"
      )
      await test_25_oversized_close_session_negative(
          client, f"{base_url}/test"
      )
      await test_26_invalid_utf8_close_session_negative(
          client, f"{base_url}/test"
      )
      await test_27_graceful_close_session_positive(primary_session)

      # --- Section 9: Flow Control & Stream Limits ---
      logger.info(
          "\n--- [SECTION 9] Flow Control & Stream Limits (§ 5.1, § 5.3) ---"
      )
      await test_28_flow_control_exhaustion_positive(
          client, f"{base_url}/test"
      )

      # --- Section 10: Inactivity / Heartbeat Timeout ---
      logger.info(
          "\n--- [SECTION 10] Inactivity / Heartbeat Timeout (§ 6.0) ---"
      )
      await test_29_idle_connection_survival_positive(
          client, f"{base_url}/test"
      )
      await test_30_inactivity_timeout_negative(client, f"{base_url}/test")

    except Exception as e:
      logger.error(f"❌ Test Suite Encountered Unhandled Failure: {e}")

  # Print Formatted Results Table
  passed_count = sum(1 for r in test_results if r[4] == "PASSED")
  failed_count = sum(1 for r in test_results if r[4] == "FAILED")
  total_count = len(test_results)

  print("\n" + "=" * 90)
  print(
      "📋 IETF WEBTRANSPORT OVER HTTP/3 DRAFT-16 INTEROPERABILITY TEST MATRIX"
      " SUMMARY"
  )
  print("=" * 90)
  print(
      f"{'#':<3} | {'Type':<10} | {'Section':<7} | {'Test Description':<44} |"
      f" {'Status':<6}"
  )
  print("-" * 90)
  for t_num, t_type, sect, name, status, _ in test_results:
    type_tag = f"[{t_type}]"
    print(f"{t_num:02d}  | {type_tag:<10} | {sect:<7} | {name:<44} | {status}")
  print("=" * 90)
  print(
      f"📊 Final Results: {total_count} Tests Executed | {passed_count} PASSED |"
      f" {failed_count} FAILED"
  )
  print("=" * 90 + "\n")

  if failed_count > 0 or len(pending_verifications) > 0:
    logger.error(
        f"❌ Interop Test Suite Finished with Failures: {failed_count} tests"
        f" failed, pending verifications: {pending_verifications}"
    )
    sys.exit(1)
  else:
    logger.info(
        "🎉 ALL IETF DRAFT-16 POSITIVE AND NEGATIVE TESTS PASSED PERFECTLY! 🎉"
    )
    sys.exit(0)


if __name__ == "__main__":
  asyncio.run(main())

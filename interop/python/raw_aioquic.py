"""Small raw-wire WebTransport client used by negative interop tests.

pywebtransport intentionally exposes a high-level API. Tests which inject an
unknown session identifier or a malformed capsule need lower-level control, so
they use aioquic directly while all normal interop coverage remains on
pywebtransport 0.20.1.
"""

import asyncio
import ssl
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncIterator
from urllib.parse import urlparse

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, QuicEvent, StreamReset


class RawWebTransportProtocol(QuicConnectionProtocol):
  """Expose only the wire operations required by the negative test cases."""

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.http = H3Connection(self._quic, enable_webtransport=True)
    self._stream_events = defaultdict(asyncio.Queue)
    self._terminated = asyncio.Event()
    self._termination_event = None

  def quic_event_received(self, event: QuicEvent) -> None:
    if isinstance(event, ConnectionTerminated):
      self._termination_event = event
      self._terminated.set()
    elif isinstance(event, StreamReset):
      self._stream_events[event.stream_id].put_nowait(event)

    for http_event in self.http.handle_event(event):
      stream_id = getattr(http_event, "stream_id", None)
      if stream_id is not None:
        self._stream_events[stream_id].put_nowait(http_event)

  async def connect_webtransport(self, *, authority: str, path: str) -> int:
    """Open an extended CONNECT stream and return its session identifier."""
    stream_id = self._quic.get_next_available_stream_id()
    self.http.send_headers(
        stream_id=stream_id,
        headers=[
            (b":method", b"CONNECT"),
            (b":scheme", b"https"),
            (b":authority", authority.encode("ascii")),
            (b":path", path.encode("ascii")),
            (b":protocol", b"webtransport"),
        ],
    )
    self.transmit()

    while True:
      event = await self._next_stream_event(stream_id=stream_id, timeout=5.0)
      if isinstance(event, StreamReset):
        raise ConnectionError(
            f"CONNECT stream reset with error code {hex(event.error_code)}"
        )
      if isinstance(event, HeadersReceived):
        status = dict(event.headers).get(b":status")
        if status != b"200":
          raise ConnectionError(f"CONNECT rejected with status {status!r}")
        return stream_id

  def send_capsule(self, *, session_id: int, data: bytes) -> None:
    """Send encoded capsule bytes on a session's CONNECT stream."""
    self.http.send_data(stream_id=session_id, data=data, end_stream=False)
    self.transmit()

  def send_unknown_session_stream(
      self, *, session_id: int, data: bytes, unidirectional: bool
  ) -> None:
    """Send a WebTransport stream carrying an arbitrary session identifier."""
    stream_id = self.http.create_webtransport_stream(
        session_id=session_id, is_unidirectional=unidirectional
    )
    self._quic.send_stream_data(stream_id, data, end_stream=True)
    self.transmit()

  def send_unknown_session_datagram(
      self, *, quarter_session_id: int, data: bytes
  ) -> None:
    """Send an HTTP Datagram carrying an arbitrary quarter stream ID."""
    self.http.send_datagram(stream_id=quarter_session_id * 4, data=data)
    self.transmit()

  async def exchange(self, *, session_id: int, data: bytes) -> bytes:
    """Exchange data on a valid raw bidirectional WebTransport stream."""
    stream_id = self.http.create_webtransport_stream(
        session_id=session_id, is_unidirectional=False
    )
    self._quic.send_stream_data(stream_id, data, end_stream=False)
    self.transmit()

    received = bytearray()
    while True:
      event = await self._next_stream_event(stream_id=stream_id, timeout=2.0)
      if isinstance(event, StreamReset):
        raise ConnectionError(
            f"WebTransport stream reset with error code {hex(event.error_code)}"
        )
      if isinstance(event, WebTransportStreamDataReceived):
        received.extend(event.data)
        if received or event.stream_ended:
          break

    self._quic.send_stream_data(stream_id, b"", end_stream=True)
    self.transmit()
    return bytes(received)

  async def _next_stream_event(self, *, stream_id: int, timeout: float):
    if self._termination_event is not None:
      raise ConnectionError(
          "QUIC connection terminated: "
          f"{self._termination_event.reason_phrase} "
          f"({hex(self._termination_event.error_code)})"
      )

    stream_task = asyncio.create_task(self._stream_events[stream_id].get())
    termination_task = asyncio.create_task(self._terminated.wait())
    done, pending = await asyncio.wait(
        {stream_task, termination_task},
        timeout=timeout,
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
      task.cancel()

    if not done:
      raise TimeoutError(f"Timed out waiting for stream {stream_id}")
    if termination_task in done:
      stream_task.cancel()
      event = self._termination_event
      raise ConnectionError(
          "QUIC connection terminated"
          if event is None
          else (
              f"QUIC connection terminated: {event.reason_phrase} "
              f"({hex(event.error_code)})"
          )
      )
    return stream_task.result()


@asynccontextmanager
async def open_raw_session(
    url: str,
) -> AsyncIterator[tuple[RawWebTransportProtocol, int]]:
  """Open one raw HTTP/3 WebTransport session for wire-level checks."""
  parsed = urlparse(url)
  if parsed.scheme != "https" or parsed.hostname is None:
    raise ValueError(f"Invalid WebTransport URL: {url}")

  port = parsed.port or 443
  authority = parsed.netloc
  configuration = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
  configuration.verify_mode = ssl.CERT_NONE
  configuration.max_datagram_frame_size = 65536

  async with connect(
      parsed.hostname,
      port,
      configuration=configuration,
      create_protocol=RawWebTransportProtocol,
  ) as protocol:
    if not isinstance(protocol, RawWebTransportProtocol):
      raise TypeError(f"Unexpected QUIC protocol: {type(protocol).__name__}")
    session_id = await protocol.connect_webtransport(
        authority=authority, path=parsed.path or "/"
    )
    yield protocol, session_id

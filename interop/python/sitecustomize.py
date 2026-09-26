import asyncio
from collections import defaultdict
import pywebtransport
from pywebtransport.events import Event, EventType
from pywebtransport.protocol.handler import WebTransportProtocolHandler
from pywebtransport.stream.stream import WebTransportStream, WebTransportSendStream
from pywebtransport.session.session import WebTransportSession

# 1. Patch WebTransportSendStream.write_all so it honors end_stream properly without closing prematurely
async def _patched_write_all(self, data, *args, end_stream=False, chunk_size=8192, **kwargs):
    if not data:
        if end_stream and not self.is_closed:
            await self.close()
        return
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        is_last = (i + chunk_size >= len(data)) and end_stream
        await self.write(chunk, end_stream=is_last)
    if end_stream and not self.is_closed:
        await self.close()

WebTransportSendStream.write_all = _patched_write_all
WebTransportStream.write_all = _patched_write_all

# 2. Patch WebTransportSession.send_datagram
async def _patched_send_datagram(self, data, *args, **kwargs):
    await self.datagrams.send(data)

WebTransportSession.send_datagram = _patched_send_datagram

# 3. Patch WebTransportProtocolHandler event queuing so events emitted before listeners attach are not lost
_orig_proto_init = WebTransportProtocolHandler.__init__
def _patched_proto_init(self, *args, **kwargs):
    _orig_proto_init(self, *args, **kwargs)
    self._buffered_events = defaultdict(list)

WebTransportProtocolHandler.__init__ = _patched_proto_init

_orig_proto_emit = WebTransportProtocolHandler.emit
async def _patched_proto_emit(self, event_type, data=None, source=None):
    has_listeners = bool(self.listeners(event_type))
    et_str = event_type.value if isinstance(event_type, EventType) else str(event_type)
    if not has_listeners and (
        et_str == EventType.STREAM_OPENED.value
        or et_str.startswith("stream_data_received:")
        or et_str.startswith("stream_closed:")
    ):
        event = Event(type=event_type, data=data, source=source)
        self._buffered_events[event_type].append(event)
        return
    await _orig_proto_emit(self, event_type, data=data, source=source)

WebTransportProtocolHandler.emit = _patched_proto_emit

_orig_proto_on = WebTransportProtocolHandler.on
def _patched_proto_on(self, event_type, handler):
    _orig_proto_on(self, event_type, handler)
    if hasattr(self, '_buffered_events') and event_type in self._buffered_events:
        buffered = self._buffered_events.pop(event_type)
        for ev in buffered:
            if asyncio.iscoroutinefunction(handler):
                asyncio.create_task(handler(ev))
            else:
                handler(ev)

WebTransportProtocolHandler.on = _patched_proto_on

# 4. Patch accept_unidirectional_stream and accept_bidirectional_stream
async def _accept_unidirectional_stream(self):
    if not hasattr(self, '_uni_queue'):
        self._uni_queue = asyncio.Queue()
    if not hasattr(self, '_bidi_queue'):
        self._bidi_queue = asyncio.Queue()

    while self._uni_queue.empty():
        s = await self._incoming_streams.get()
        if s is None:
            raise EOFError("Session closed")
        if isinstance(s, WebTransportStream):
            await self._bidi_queue.put(s)
        else:
            await self._uni_queue.put(s)
    return await self._uni_queue.get()

async def _accept_bidirectional_stream(self):
    if not hasattr(self, '_uni_queue'):
        self._uni_queue = asyncio.Queue()
    if not hasattr(self, '_bidi_queue'):
        self._bidi_queue = asyncio.Queue()

    while self._bidi_queue.empty():
        s = await self._incoming_streams.get()
        if s is None:
            raise EOFError("Session closed")
        if isinstance(s, WebTransportStream):
            await self._bidi_queue.put(s)
        else:
            await self._uni_queue.put(s)
    return await self._bidi_queue.get()

WebTransportSession.accept_unidirectional_stream = _accept_unidirectional_stream
WebTransportSession.accept_bidirectional_stream = _accept_bidirectional_stream

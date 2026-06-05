import asyncio
import ssl
import logging
import time

# Configure logging (Set to INFO to keep our custom capsule logs highly visible)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WebTransportTest")

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType
import aioquic.h3.connection
from aioquic.h3.connection import H3Connection
from aioquic.buffer import Buffer


# --- 1. Capsule Parser ---

def decode_varint(data: bytes, offset: int = 0) -> tuple:
    if offset >= len(data):
        return -1, offset
    first = data[offset]
    length = 1 << (first >> 6)
    if offset + length > len(data):
        return -1, offset
    val_bytes = data[offset: offset + length]
    if length == 1:
        val = val_bytes[0] & 0x3F
    elif length == 2:
        val = int.from_bytes(val_bytes, "big") & 0x3FFF
    elif length == 4:
        val = int.from_bytes(val_bytes, "big") & 0x3FFFFFFF
    else:
        val = int.from_bytes(val_bytes, "big") & 0x3FFFFFFFFFFFFFFF
    return val, offset + length


def parse_capsule(data: bytes):
    try:
        offset = 0
        while offset < len(data):
            cap_type, offset = decode_varint(data, offset)
            if cap_type == -1:
                break
            cap_len, offset = decode_varint(data, offset)
            if cap_len == -1 or offset + cap_len > len(data):
                break
            cap_val_bytes = data[offset: offset + cap_len]
            offset += cap_len

            type_str = f"0x{cap_type:X}"
            val_str = cap_val_bytes.hex()

            if cap_type == 0x190B4D3F:
                type_str = "WT_MAX_STREAMS (Bidirectional)"
                val, _ = decode_varint(cap_val_bytes)
                val_str = str(val)
            elif cap_type == 0x190B4D40:
                type_str = "WT_MAX_STREAMS (Unidirectional)"
                val, _ = decode_varint(cap_val_bytes)
                val_str = str(val)
            elif cap_type == 0x190B4D44:
                type_str = "WT_STREAMS_BLOCKED (Bidirectional)"
                val, _ = decode_varint(cap_val_bytes)
                val_str = str(val)
            elif cap_type == 0x190B4D45:
                type_str = "WT_STREAMS_BLOCKED (Unidirectional)"
                val, _ = decode_varint(cap_val_bytes)
                val_str = str(val)
            elif cap_type == 0x2843:
                type_str = "CLOSE_WEBTRANSPORT_SESSION"

            print(f"💊 [CAPSULE INTERCEPTED] Type: {type_str} | Length: {cap_len} | Value: {val_str}")
    except Exception as e:
        print(f"❌ Error parsing capsule bytes: {e}")


# --- 2. Low-Level Monkey-Patching (Interception Layer) ---

# Save reference to original event handler
_original_handle_event = H3Connection.handle_event


def patched_handle_event(self, event):
    # Intercept raw stream data before HTTP/3 parses it
    if type(event).__name__ == "StreamDataReceived":
        try:
            # Create a non-destructive buffer reader over the incoming data chunk
            buf = Buffer(data=event.data)
            while not buf.eof():
                # Read the H3 Frame Type and Length
                frame_type = buf.pull_uint_var()
                frame_length = buf.pull_uint_var()

                # 0x4752 is the standard HTTP/3 CAPSULE frame type identifier
                if frame_type == 0x4752 and frame_length <= buf.bytes_remaining():
                    capsule_bytes = buf.data[buf.tell():buf.tell() + frame_length]
                    parse_capsule(capsule_bytes)

                # Advance past the payload to check for the next frame in the buffer chunk
                buf.seek(buf.tell() + frame_length)
        except Exception:
            # If the framing layout is partial/segmented across packets, we ignore it here.
            # The original aioquic code will handle stream buffering and reassembly safely.
            pass

    # Forward the event back to the original handler so connection execution flow remains normal
    return _original_handle_event(self, event)


# Inject our hook globally into aioquic
aioquic.h3.connection.H3Connection.handle_event = patched_handle_event


# --- 3. WebTransport Application Logic ---

async def receive_datagrams(session):
    """Continuously listens for incoming datagrams."""
    try:
        while True:
            event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED)
            if isinstance(event.data, dict) and (d := event.data.get("data")):
                print(f"📥 [DATAGRAM RECEIVED]: {d!r}")
    except asyncio.CancelledError:
        pass


async def receive_server_pushes(session):
    """Listens for new unidirectional streams opened by the server."""
    try:
        async for stream in session.incoming_unidirectional_streams():
            print(f"🚀 [NEW SERVER UNI STREAM] ID: {stream.stream_id}")
            asyncio.create_task(read_stream(stream, "UNI_SERVER"))
    except asyncio.CancelledError:
        pass


async def receive_server_pushes_bi(session):
    """Listens for new bidirectional streams opened by the server."""
    try:
        async for stream in session.incoming_bidirectional_streams():
            print(f"🚀 [NEW SERVER BIDI STREAM] ID: {stream.stream_id}")
            asyncio.create_task(read_stream(stream, "BIDI_SERVER"))
    except asyncio.CancelledError:
        pass


async def read_stream(stream, stream_type: str):
    """Helper to drain incoming data from a stream until EOF."""
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                print(f"🛑 [{stream_type} STREAM ENDED] ID: {stream.stream_id}")
                break
            print(f"📥 [{stream_type} DATA] ID: {stream.stream_id}: {chunk!r}")
    except Exception as e:
        print(f"Error reading {stream_type} stream {stream.stream_id}: {e}")


async def push_uni(stream):
    """Periodically pushes data out a client uni stream."""
    try:
        while True:
            msg = f"CLIENT_UNI {time.time_ns()}\n".encode()
            await stream.write_all(data=msg, end_stream=False)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        await stream.write_all(data=b"", end_stream=True)


async def push_bidi(stream):
    """Periodically pushes data out a client bidi stream and monitors returns."""
    asyncio.create_task(read_stream(stream, "CLIENT_BIDI_ECHO"))
    try:
        while True:
            msg = f"CLIENT_BIDI {time.time_ns()}\n".encode()
            await stream.write_all(data=msg, end_stream=False)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        await stream.write_all(data=b"", end_stream=True)


# --- 4. Main Event Execution ---

async def main() -> None:
    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="DEBUG")

    async with WebTransportClient(config=config) as client:
        print("🔗 Connecting to server via WebTransport...")
        session = await client.connect(url="https://127.0.0.1:4433/")
        print("✅ Connection Established!")
        print("🔍 [SESSION DIAGNOSTICS]", await session.diagnostics())

        # Start persistent monitoring loops
        bg_tasks = [
            asyncio.create_task(receive_datagrams(session)),
            asyncio.create_task(receive_server_pushes(session)),
            asyncio.create_task(receive_server_pushes_bi(session))
        ]

        # Initial validation fires
        await session.send_datagram(data=b"Hello, Datagram initial fire!")
        uni_stream = await session.create_unidirectional_stream()
        bidi_stream = await session.create_bidirectional_stream()

        # Start periodic data streams
        push_tasks = [
            asyncio.create_task(push_uni(uni_stream)),
            asyncio.create_task(push_bidi(bidi_stream))
        ]

        try:
            print("\n🧪 Active Traffic Engine Online. Press Ctrl+C to terminate cleanly.\n")
            while True:
                await asyncio.sleep(3600)  # Keeps main thread suspended safely
        except asyncio.CancelledError:
            print("\nShutting down stream loops gracefully...")
            for task in bg_tasks + push_tasks:
                task.cancel()
            await asyncio.gather(*bg_tasks, *push_tasks, return_exceptions=True)
        finally:
            print("🚪 Closing down WebTransport session cleanly.")
            await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest terminated cleanly by operator.")
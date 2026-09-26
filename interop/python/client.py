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
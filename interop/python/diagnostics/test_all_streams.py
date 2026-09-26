#!/usr/bin/env python3
"""
Comprehensive WebTransport Client Test Script.
Tests and logs all stream types:
- Client-Initiated Bidirectional
- Client-Initiated Unidirectional
- Server-Initiated Bidirectional
- Server-Initiated Unidirectional
- Datagrams
"""

import asyncio
import ssl
import logging

# Configure logging with clear formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("AllStreamsTest")

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType
import aioquic.h3.connection

# ── HACK for aioquic WebTransport Datagrams ──
_original_handle_event = aioquic.h3.connection.H3Connection.handle_event

def patched_handle_event(self, event):
    if type(event).__name__ == "DatagramReceived":
        try:
            self._session.events.put_nowait({
                "type": EventType.DATAGRAM_RECEIVED,
                "data": event.data
            })
        except Exception:
            pass
    return _original_handle_event(self, event)

aioquic.h3.connection.H3Connection.handle_event = patched_handle_event

async def read_stream_forever(stream, label: str):
    """Continuously read from a stream and log any data received."""
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                logger.info(f"🛑 [{label}] Stream ended (FIN received)")
                break
            text = chunk.decode("utf-8", errors="replace")
            logger.info(f"📥 [{label}] Received: {text!r}")
    except Exception as e:
        logger.error(f"❌ [{label}] Error reading stream: {e}")

async def listen_datagrams(session):
    """Continuously listen for incoming Datagrams."""
    try:
        while True:
            event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED)
            if isinstance(event.data, dict) and (d := event.data.get("data")):
                text = d.decode("utf-8", errors="replace") if isinstance(d, bytes) else str(d)
                logger.info(f"☄️ [DATAGRAM] Received: {text!r}")
    except Exception as e:
        logger.error(f"❌ [DATAGRAM] Error receiving: {e}")

async def listen_server_uni_streams(session):
    """Listen for Server-Initiated Unidirectional streams."""
    try:
        async for stream in session.incoming_unidirectional_streams():
            logger.info(f"🚀 [SERVER-UNI] Server opened Unidirectional stream (ID: {stream.stream_id})")
            asyncio.create_task(read_stream_forever(stream, f"SERVER-UNI {stream.stream_id}"))
    except asyncio.CancelledError:
        pass

async def listen_server_bidi_streams(session):
    """Listen for Server-Initiated Bidirectional streams."""
    try:
        async for stream in session.incoming_bidirectional_streams():
            logger.info(f"🚀 [SERVER-BIDI] Server opened Bidirectional stream (ID: {stream.stream_id})")
            # Setup a background task to read from this stream
            asyncio.create_task(read_stream_forever(stream, f"SERVER-BIDI {stream.stream_id}"))
            
            # Write a response back on the same stream!
            response = f"ACK from Python Client for Bidi Stream {stream.stream_id}"
            await stream.write_all(data=response.encode("utf-8"), end_stream=False)
            logger.info(f"↗️  [SERVER-BIDI {stream.stream_id}] Sent response: {response!r}")
    except asyncio.CancelledError:
        pass

async def main():
    config = ClientConfig(
        verify_mode=ssl.CERT_NONE,
        log_level="WARNING", # Hide pywebtransport internal logs to focus on our app logic
    )

    logger.info("🔗 Connecting to WebTransport server at https://localhost:4433/test ...")
    
    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/test")
        logger.info("✅ Connected successfully!\n")

        # Start background listeners for server-initiated events
        bg_tasks = [
            asyncio.create_task(listen_server_uni_streams(session)),
            asyncio.create_task(listen_server_bidi_streams(session)),
            asyncio.create_task(listen_datagrams(session)),
        ]

        await asyncio.sleep(0.5) # Small delay to let server streams arrive

        # ── 1. Client-Initiated Bidirectional Stream ──
        logger.info("=" * 60)
        logger.info("🧪 1. Testing Client-Initiated Bidirectional Stream")
        client_bidi = await session.create_bidirectional_stream()
        logger.info(f"👉 Created Client BIDI Stream (ID: {client_bidi.stream_id})")
        
        bidi_msg = "Hello Bidi from Python!"
        logger.info(f"↗️  [CLIENT-BIDI] Sending: {bidi_msg!r}")
        await client_bidi.write_all(data=bidi_msg.encode("utf-8"), end_stream=False)
        
        # Read the echo response from the server
        asyncio.create_task(read_stream_forever(client_bidi, "CLIENT-BIDI"))
        await asyncio.sleep(0.5)


        # ── 2. Client-Initiated Unidirectional Stream ──
        logger.info("=" * 60)
        logger.info("🧪 2. Testing Client-Initiated Unidirectional Stream")
        client_uni = await session.create_unidirectional_stream()
        logger.info(f"👉 Created Client UNI Stream (ID: {client_uni.stream_id})")
        
        uni_msg = "Hello Uni from Python (Fire and Forget)!"
        logger.info(f"↗️  [CLIENT-UNI] Sending: {uni_msg!r}")
        await client_uni.write_all(data=uni_msg.encode("utf-8"), end_stream=False)
        await asyncio.sleep(0.5)


        # ── 3. Datagrams ──
        logger.info("=" * 60)
        logger.info("🧪 3. Testing Datagrams")
        dg_msg = "Hello Datagram from Python!"
        logger.info(f"↗️  [DATAGRAM] Sending: {dg_msg!r}")
        await session.send_datagram(data=dg_msg.encode("utf-8"))
        await asyncio.sleep(0.5)

        logger.info("=" * 60)
        logger.info("⏳ Waiting for 3 seconds before closing to receive any delayed messages...")
        await asyncio.sleep(3)
        
        # Cancel background tasks
        for task in bg_tasks:
            task.cancel()

        logger.info("👋 Closing connection.")

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Minimal WebTransport connection test — diagnoses if session handshake works.
Run: python3 test_chat_simple.py
"""

import asyncio
import ssl
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s │ %(name)-40s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("SimpleTest")

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType


async def main():
    config = ClientConfig(
        verify_mode=ssl.CERT_NONE,
        log_level="DEBUG",
    )

    logger.info("🔗 Creating WebTransport client...")
    async with WebTransportClient(config=config) as client:
        logger.info("🔗 Connecting to https://localhost:4433/chat ...")
        try:
            session = await client.connect(
                url="https://localhost:4433/chat",
                # Some versions of pywebtransport accept a timeout param
            )
            logger.info(f"✅ SESSION READY! Session: {session}")
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            logger.info("Trying /test path instead...")
            try:
                session = await client.connect(url="https://localhost:4433/test")
                logger.info(f"✅ /test path SESSION READY! Session: {session}")
            except Exception as e2:
                logger.error(f"❌ /test also failed: {e2}")
                return

        # If we get here, try basic operations
        try:
            logger.info("📤 Sending datagram...")
            await session.send_datagram(data=b"PING")
            logger.info("✅ Datagram sent!")
        except Exception as e:
            logger.error(f"Datagram failed: {e}")

        try:
            logger.info("📤 Creating bidirectional stream...")
            stream = await session.create_bidirectional_stream()
            logger.info(f"✅ Bidi stream created! ID: {stream.stream_id}")
            await stream.write_all(data=b"\x01JOIN lobby Tester", end_stream=False)
            logger.info("✅ Data written to stream!")

            # Try reading response
            logger.info("📥 Reading response (5s timeout)...")
            try:
                chunk = await asyncio.wait_for(stream.read(), timeout=5.0)
                if chunk:
                    logger.info(f"✅ Got response: {chunk!r}")
                else:
                    logger.info("Stream returned empty (EOF)")
            except asyncio.TimeoutError:
                logger.warning("⏱️ Read timed out after 5s (server may not have replied)")
        except Exception as e:
            logger.error(f"Stream failed: {e}")

        logger.info("🏁 Test complete, closing session...")
        await session.close()

    logger.info("👋 Done!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCancelled.")

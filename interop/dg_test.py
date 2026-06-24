import asyncio
import ssl
import logging
import time

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType

# Configure test-like logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("TestDatagrams")

async def receive_datagrams(session):
    """Continuously listens for incoming datagrams."""
    logger.info("🎧 Listening for incoming Datagrams...")
    try:
        while True:
            event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED)
            if isinstance(event.data, dict) and (d := event.data.get("data")):
                logger.info(f"📥 [DATAGRAM IN]: {d!r}")
    except asyncio.CancelledError:
        logger.info("🛑 Datagram receiver shutting down.")

async def send_datagrams(session):
    """Continuously sends datagrams."""
    logger.info("🚀 Starting continuous Datagram transmission...")
    seq = 0
    try:
        while True:
            seq += 1
            msg = f"TEST_DATAGRAM_SEQ_{seq}_{time.time_ns()}".encode()
            await session.send_datagram(data=msg)
            logger.info(f"📤 [DATAGRAM OUT]: {msg!r}")
            await asyncio.sleep(1) # Adjust rate here
    except asyncio.CancelledError:
        logger.info("🛑 Datagram sender shutting down.")

async def main() -> None:
    config = ClientConfig(verify_mode=ssl.CERT_NONE)
    url = "https://localhost:4433/test"

    async with WebTransportClient(config=config) as client:
        logger.info(f"🔗 Connecting to {url}...")
        session = await client.connect(url=url)
        logger.info("✅ Connection Established!")

        tasks = [
            asyncio.create_task(receive_datagrams(session)),
            asyncio.create_task(send_datagrams(session))
        ]

        try:
            logger.info("\n🧪 Datagram Traffic Engine Online. Press Ctrl+C to terminate.\n")
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for task in tasks:
                task.cancel()
            logger.info("🚪 Closing down WebTransport session cleanly.")
            await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest terminated cleanly by operator.")
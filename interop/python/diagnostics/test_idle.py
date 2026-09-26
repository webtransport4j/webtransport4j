import asyncio
import ssl
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(name)-18s │ %(levelname)-5s │ %(message)s")
logger = logging.getLogger("TestIdle")

from pywebtransport import ClientConfig, WebTransportClient

async def read_stream(stream, label):
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                logger.info(f"🛑 [{label}] Stream ended")
                break
            logger.info(f"📥 [{label}] ← {chunk}")
    except Exception as e:
        logger.error(f"❌ [{label}] error: {e}")

async def main():
    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")
    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/chat")
        logger.info("✅ Connected to /chat")
        
        control_stream = await session.create_bidirectional_stream()
        await control_stream.write_all(data=bytes([0x01]), end_stream=False)
        asyncio.create_task(read_stream(control_stream, "CTRL"))
        
        logger.info("Waiting 10 seconds...")
        await asyncio.sleep(10)
        logger.info("Sending JOIN...")
        await control_stream.write_all(data=b"JOIN room a", end_stream=False)
        await asyncio.sleep(2)
        logger.info("Done.")

asyncio.run(main())

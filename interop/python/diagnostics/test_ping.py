import asyncio
import ssl
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(name)-18s │ %(levelname)-5s │ %(message)s")
logger = logging.getLogger("TestPing")

from pywebtransport import ClientConfig, WebTransportClient

async def read_stream_forever(stream, label):
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                logger.info(f"🛑 [{label}] Stream ended")
                break
    except Exception as e:
        logger.error(f"❌ [{label}] error: {e}")

async def main():
    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="DEBUG")
    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/chat")
        
        control_stream = await session.create_bidirectional_stream()
        await control_stream.write_all(data=bytes([0x01]), end_stream=False)
        asyncio.create_task(read_stream_forever(control_stream, "CTRL"))
        
        await asyncio.sleep(6)
        logger.info("Test finished")

asyncio.run(main())

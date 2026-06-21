import asyncio
import ssl
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(name)-18s │ %(levelname)-5s │ %(message)s")
logger = logging.getLogger("TestVoice")

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
    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")
    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/chat")
        
        control_stream = await session.create_bidirectional_stream()
        await control_stream.write_all(data=bytes([0x01]), end_stream=False)
        asyncio.create_task(read_stream_forever(control_stream, "CTRL"))
        
        chat_stream = await session.create_bidirectional_stream()
        await chat_stream.write_all(data=bytes([0x02]), end_stream=False)
        asyncio.create_task(read_stream_forever(chat_stream, "CHAT"))

        voice_stream = await session.create_unidirectional_stream()
        await voice_stream.write_all(data=bytes([0x03]), end_stream=False)
        
        logger.info("Waiting 10 seconds...")
        await asyncio.sleep(10)
        logger.info("Done.")

asyncio.run(main())

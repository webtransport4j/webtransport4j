import asyncio
import ssl
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(name)-18s │ %(levelname)-5s │ %(message)s")
logger = logging.getLogger("TestIdle")

from pywebtransport import ClientConfig, WebTransportClient

async def read_stream_forever(stream, label):
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                logger.info(f"🛑 [{label}] Stream ended")
                break
            logger.info(f"📥 [{label}] ← {len(chunk)} bytes")
    except Exception as e:
        logger.error(f"❌ [{label}] error: {e}")

async def listen_uni(session):
    async for stream in session.incoming_unidirectional_streams():
        asyncio.create_task(read_stream_forever(stream, "UNI-RX"))

async def listen_bidi(session):
    async for stream in session.incoming_bidirectional_streams():
        asyncio.create_task(read_stream_forever(stream, "BIDI-RX"))

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
        
        asyncio.create_task(listen_uni(session))
        asyncio.create_task(listen_bidi(session))
        
        logger.info("Waiting 10 seconds...")
        await asyncio.sleep(10)
        logger.info("Done.")

asyncio.run(main())

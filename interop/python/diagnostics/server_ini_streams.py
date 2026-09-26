import asyncio
import ssl
import logging
import time

from pywebtransport import ClientConfig, WebTransportClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("TestServerStreams")

async def handle_server_uni(stream):
    """Reads incoming data from a server-initiated uni stream."""
    logger.info(f"🚀 [SERVER UNI IN] Accepting Stream ID: {stream.stream_id}")
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                logger.info(f"🛑 [SERVER UNI IN] Stream EOF ID: {stream.stream_id}")
                break
            logger.info(f"📥 [SERVER UNI IN] ID: {stream.stream_id} | Data: {chunk!r}")
    except Exception as e:
        logger.error(f"Error reading server uni {stream.stream_id}: {e}")

async def handle_server_bidi_read(stream):
    """Reads incoming data from a server-initiated bidi stream."""
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                logger.info(f"🛑 [SERVER BIDI IN] Stream EOF ID: {stream.stream_id}")
                break
            logger.info(f"📥 [SERVER BIDI IN] ID: {stream.stream_id} | Data: {chunk!r}")
    except Exception as e:
        logger.error(f"Error reading server bidi {stream.stream_id}: {e}")

async def handle_server_bidi_write(stream):
    """Sends continuous responses back on a server-initiated bidi stream."""
    seq = 0
    try:
        while True:
            seq += 1
            msg = f"CLIENT_REPLY_ON_SERVER_BIDI_SEQ_{seq}_{time.time_ns()}\n".encode()
            await stream.write_all(data=msg, end_stream=False)
            logger.info(f"📤 [SERVER BIDI OUT] ID: {stream.stream_id} | Data: {msg.strip()!r}")
            await asyncio.sleep(2)
    except asyncio.CancelledError:
        await stream.write_all(data=b"", end_stream=True)
        logger.info(f"🛑 [SERVER BIDI OUT] Closed writing for ID: {stream.stream_id}")

async def listen_for_server_uni_streams(session):
    """Background task to accept all new server uni streams."""
    logger.info("🎧 Awaiting Server-initiated Unidirectional Streams...")
    try:
        async for stream in session.incoming_unidirectional_streams():
            asyncio.create_task(handle_server_uni(stream))
    except asyncio.CancelledError:
        pass

async def listen_for_server_bidi_streams(session):
    """Background task to accept all new server bidi streams."""
    logger.info("🎧 Awaiting Server-initiated Bidirectional Streams...")
    try:
        async for stream in session.incoming_bidirectional_streams():
            logger.info(f"🚀 [SERVER BIDI] Accepting Stream ID: {stream.stream_id}")
            asyncio.create_task(handle_server_bidi_read(stream))
            asyncio.create_task(handle_server_bidi_write(stream))
    except asyncio.CancelledError:
        pass

async def main() -> None:
    config = ClientConfig(verify_mode=ssl.CERT_NONE)
    url = "https://localhost:4433/test"

    async with WebTransportClient(config=config) as client:
        logger.info(f"🔗 Connecting to {url}...")
        session = await client.connect(url=url)
        logger.info("✅ Connection Established!")

        tasks = [
            asyncio.create_task(listen_for_server_uni_streams(session)),
            asyncio.create_task(listen_for_server_bidi_streams(session))
        ]

        try:
            logger.info("\n🧪 Server-Initiated Stream Engine Online. Press Ctrl+C to terminate.\n")
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
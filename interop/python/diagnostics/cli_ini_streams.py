import asyncio
import ssl
import logging
import time

from pywebtransport import ClientConfig, WebTransportClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("TestClientStreams")

async def push_uni_stream(stream):
    """Continuously pushes data out a client-initiated uni stream."""
    logger.info(f"🚀 [UNI OUT] Started transmitting on Stream ID: {stream.stream_id}")
    seq = 0
    try:
        while True:
            seq += 1
            msg = f"CLIENT_UNI_SEQ_{seq}_{time.time_ns()}\n".encode()
            await stream.write_all(data=msg, end_stream=False)
            logger.info(f"📤 [UNI OUT] ID: {stream.stream_id} | Data: {msg.strip()!r}")
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        await stream.write_all(data=b"", end_stream=True)
        logger.info(f"🛑 [UNI OUT] Stream ID: {stream.stream_id} closed.")

async def push_bidi_stream(stream):
    """Continuously pushes data out a client-initiated bidi stream."""
    logger.info(f"🚀 [BIDI OUT] Started transmitting on Stream ID: {stream.stream_id}")
    seq = 0
    try:
        while True:
            seq += 1
            msg = f"CLIENT_BIDI_SEQ_{seq}_{time.time_ns()}\n".encode()
            await stream.write_all(data=msg, end_stream=False)
            logger.info(f"📤 [BIDI OUT] ID: {stream.stream_id} | Data: {msg.strip()!r}")
            await asyncio.sleep(1.5)
    except asyncio.CancelledError:
        await stream.write_all(data=b"", end_stream=True)
        logger.info(f"🛑 [BIDI OUT] Stream ID: {stream.stream_id} closed.")

async def read_stream(stream, tag: str):
    """Drains incoming data from a bidi stream."""
    logger.info(f"🎧 [{tag} IN] Listening on Stream ID: {stream.stream_id}")
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                logger.info(f"🛑 [{tag} IN] Stream ended by peer. ID: {stream.stream_id}")
                break
            logger.info(f"📥 [{tag} IN] ID: {stream.stream_id} | Data: {chunk!r}")
    except Exception as e:
        logger.error(f"Error reading {tag} stream {stream.stream_id}: {e}")

async def main() -> None:
    config = ClientConfig(verify_mode=ssl.CERT_NONE)
    url = "https://localhost:4433/test"

    async with WebTransportClient(config=config) as client:
        logger.info(f"🔗 Connecting to {url}...")
        session = await client.connect(url=url)
        logger.info("✅ Connection Established!")

        # Create the streams
        uni_stream = await session.create_unidirectional_stream()
        bidi_stream = await session.create_bidirectional_stream()

        # Start operations
        tasks = [
            asyncio.create_task(push_uni_stream(uni_stream)),
            asyncio.create_task(push_bidi_stream(bidi_stream)),
            asyncio.create_task(read_stream(bidi_stream, "CLIENT_BIDI"))
        ]

        try:
            logger.info("\n🧪 Client-Initiated Stream Engine Online. Press Ctrl+C to terminate.\n")
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
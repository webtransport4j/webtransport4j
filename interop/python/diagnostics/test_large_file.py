import asyncio
import ssl
from aioquic.asyncio.client import connect
from aioquic.quic.configuration import QuicConfiguration

CHUNK_SIZE = 64 * 1024 * 10


async def send_file(host, port, filename):
    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=["h3"],   # Match the server ALPN
        verify_mode=ssl.CERT_NONE,
    )

    async with connect(
            host,
            port,
            configuration=configuration,
    ) as client:

        # FIX 1: Use the high-level asyncio API instead of private _quic internals
        reader, writer = await client.create_stream()

        # NOTE: For even better async performance with massive files, consider
        # using `aiofiles` so file I/O doesn't block the loop either.
        with open(filename, "rb") as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break

                # FIX 2: Write data to the asyncio stream writer
                writer.write(data)

                # FIX 3: Yield control back to the event loop.
                # This respects the QUIC flow control window and prevents RAM bloat!
                await writer.drain()

        # FIX 4: Send the FIN bit gracefully
        writer.write_eof()
        writer.close()

        # Ensure the stream closes cleanly
        await writer.wait_closed()

        print("Finished sending.")

        # Wait a little so the underlying connection can close cleanly
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(
        send_file(
            "127.0.0.1",
            4242,
            "/Users/sam/Documents/GitHub/webtransport4j/interop/large-file.bin",
        )
    )
import asyncio
import ssl
import struct

from pywebtransport import ClientConfig, WebTransportClient


def encode_length_prefixed(payload: bytes) -> bytes:
    # 4-byte big-endian length prefix
    return struct.pack(">I", len(payload)) + payload


async def main():
    config = ClientConfig(
        verify_mode=ssl.CERT_NONE,
        log_level="DEBUG"
    )

    async with WebTransportClient(config=config) as client:

        print("🔗 Connecting...")
        session = await client.connect(
            url="https://127.0.0.1:4433/"
        )

        print("✅ Connected")

        # Create bidi stream
        stream = await session.create_bidirectional_stream()

        # Create one length-prefixed message
        payload = b"HELLO_LENGTH_PREFIX"

        framed = encode_length_prefixed(payload)

        print(
            f"📤 Sending payload={payload!r} "
            f"length={len(payload)}"
        )

        print(
            "📦 Raw frame:",
            framed.hex(" ")
        )

        await stream.write_all(data=framed, end_stream=False)

        print("✅ Sent")

        reply = await stream.read()
        print(f"📥 Received back: {reply!r}")

        await asyncio.sleep(5)

        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
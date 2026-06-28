import asyncio, ssl
from pywebtransport import ClientConfig, WebTransportClient
async def test():
    config = ClientConfig(verify_mode=ssl.CERT_NONE)
    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/test")
        print("CONNECTED OK")
        stream = await session.create_bidirectional_stream()
        await stream.write_all(data=b"hello", end_stream=False)
        resp = await asyncio.wait_for(stream.read(), timeout=3.0)
        print(f"GOT: {resp}")
        await session.close()
        print("DONE")
asyncio.run(test())

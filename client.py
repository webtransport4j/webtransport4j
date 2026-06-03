import asyncio
import ssl
import logging
import time
logging.basicConfig(level=logging.DEBUG)
from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType
from pywebtransport.utils import init_tracing

init_tracing()

async def receive_server_pushes(session):
    async for stream in session.incoming_unidirectional_streams():
        print("New server push stream unidirectional")

        while True:
            chunk = await stream.read()

            if not chunk:
                print("Server push stream ended")
                break

            print("Chunk:", chunk)


async def receive_server_pushes_bi(session):
    async for stream in session.incoming_bidirectional_streams():
        print("New server push stream bidirectional")

        while True:
            chunk = await stream.read()
            if not chunk:
                print("Server push stream ended")
                break

            print("Chunk:", chunk)


async def push_uni(stream):
    while True:
        msg = f"UNI {time.time_ns()}\n".encode()
        await stream.write_all(data=msg, end_stream=False)
        await asyncio.sleep(1)

async def push_bidi(stream):
    while True:
        msg = f"BIDI {time.time_ns()}\n".encode()
        await stream.write_all(data=msg, end_stream=False)
        await asyncio.sleep(1)

async def main() -> None:
    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="DEBUG")

    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://127.0.0.1:4433/")


        await session.send_datagram(data=b"Hello, Datagram!")
        print("Sending grant_data_credit...")
        await session.grant_data_credit(max_data=20_000_000)
        print("Sent grant_data_credit!")
        print("Sending grant_streams_credit (uni)...")
        await session.grant_streams_credit(is_unidirectional=True, max_streams=200)
        print("Sent grant_streams_credit (uni)!")
        print("Sending grant_streams_credit (bidi)...")
        await session.grant_streams_credit(is_unidirectional=False, max_streams=200)
        print("Sent grant_streams_credit (bidi)!")
        event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED)
        if isinstance(event.data, dict) and (data := event.data.get("data")):
            print(f"Datagram: {data!r}")

        uni_stream = await session.create_unidirectional_stream()
        bidi_stream = await session.create_bidirectional_stream()

        asyncio.create_task(push_uni(uni_stream))
        asyncio.create_task(push_bidi(bidi_stream))


        asyncio.create_task(receive_server_pushes(session))
        asyncio.create_task(receive_server_pushes_bi(session))



        while True:

            data = await bidi_stream.read()
            print(f"BIII Data: {data!r}")


            if not data:
                print("EOF")
                break

        await session.close()



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
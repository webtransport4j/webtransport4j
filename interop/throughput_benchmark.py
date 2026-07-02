import asyncio
import argparse
import ssl
import time

from pywebtransport import ClientConfig, WebTransportClient

async def discard_receiver(stream, stop_event: asyncio.Event):
    """
    Constantly drains the incoming buffer and discards it.
    This prevents QUIC flow control from blocking the sender.
    """
    try:
        while not stop_event.is_set():
            chunk = await stream.read()
            if not chunk:
                break
            # Intentionally do nothing with the chunk to maximize speed
            del chunk
    except Exception:
        pass

async def benchmark_pure_send(session, chunk_size_bytes: int, duration: float):
    chunk = b"X" * chunk_size_bytes
    bytes_sent = 0

    print(f"\n--- Starting Pure Send Throughput Benchmark ({duration}s) ---")
    stream = await session.create_bidirectional_stream()
    print("Stream opened. Commencing egress flood...")

    stop_event = asyncio.Event()

    # Start the discard receiver in the background to prevent window blocking
    receiver_task = asyncio.create_task(discard_receiver(stream, stop_event))

    start_time = time.perf_counter()
    end_time = start_time + duration

    try:
        while time.perf_counter() < end_time:
            await stream.write_all(data=chunk)
            bytes_sent += chunk_size_bytes

            # Yield control minimally to allow the event loop to drive the socket
            await asyncio.sleep(0)
    except Exception as e:
        print(f"[Sender Error] Transmit interrupted: {e}")
    finally:
        stop_event.set()
        # Allow a brief moment for the receiver task to wind down cleanly
        await asyncio.gather(receiver_task, return_exceptions=True)

    actual_duration = time.perf_counter() - start_time
    sent_mb = bytes_sent / (1024 * 1024)
    send_throughput = sent_mb / actual_duration

    print("\n================ BENCHMARK RESULTS ================")
    print(f"Actual Duration:   {actual_duration:.2f} seconds")
    print(f"Total Bytes Sent:   {bytes_sent:,} bytes")
    print(f"Total Data Sent:    {sent_mb:.2f} MB")
    print(f"Pure Send Speed:    {send_throughput:.2f} MB/s")
    print("===================================================")

async def main():
    parser = argparse.ArgumentParser(description="PyWebTransport Pure Send Benchmark")
    parser.add_argument("--url", type=str, default="https://127.0.0.1:4433/echo", help="Server URL")
    parser.add_argument("--duration", type=float, default=5.0, help="Test duration in seconds")
    parser.add_argument("--chunk-size", type=int, default=4 * 1024 * 1024, help="Chunk block size in bytes (Default: 4MB)")
    args = parser.parse_args()

    config = ClientConfig(verify_mode=ssl.CERT_NONE)

    print(f"Target URL: {args.url}")
    print("Connecting to server...")

    async with WebTransportClient(config=config) as client:
        try:
            session = await client.connect(url=args.url)
            print("Connected successfully.")
            await benchmark_pure_send(session, args.chunk_size, args.duration)
        except Exception as e:
            print(f"Benchmark run aborted: {e}")

if __name__ == "__main__":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("Optimized event loop configured via uvloop policy.")
    except ImportError:
        print("uvloop missing; defaulting to native asyncio loop framework.")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user sequence.")
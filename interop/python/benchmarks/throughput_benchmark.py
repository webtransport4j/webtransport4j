import asyncio
import argparse
import ssl
import time

from pywebtransport import ClientConfig, WebTransportClient


async def discard_receiver(stream, stop_event: asyncio.Event, stats: dict):
    """
    Continuously read echoed data so the receive window doesn't fill up.
    """
    try:
        while not stop_event.is_set():
            data = await stream.read()
            if not data:
                break
            stats["recv_bytes"] += len(data)
    except Exception:
        pass


async def benchmark(session, chunk_size: int, duration: float):
    chunk = b"X" * chunk_size

    stream = await session.create_bidirectional_stream()

    print("\n==========================================")
    print("WebTransport Throughput Benchmark")
    print("==========================================")
    print(f"Chunk Size : {chunk_size / 1024:.0f} KB")
    print(f"Duration   : {duration:.1f} seconds")
    print("==========================================\n")

    stats = {"recv_bytes": 0}
    stop_event = asyncio.Event()

    receiver = asyncio.create_task(
        discard_receiver(stream, stop_event, stats)
    )

    bytes_sent = 0
    writes = 0

    total_write_ns = 0
    max_write_ns = 0

    start = time.perf_counter()
    end = start + duration

    last_report = start
    last_bytes = 0
    last_writes = 0

    try:
        while True:
            now = time.perf_counter()

            if now >= end:
                break

            t0 = time.perf_counter_ns()

            await stream.write_all(chunk)

            elapsed_ns = time.perf_counter_ns() - t0

            total_write_ns += elapsed_ns
            max_write_ns = max(max_write_ns, elapsed_ns)

            writes += 1
            bytes_sent += chunk_size

            await asyncio.sleep(0)

            if now - last_report >= 1.0:
                delta_bytes = bytes_sent - last_bytes
                delta_writes = writes - last_writes
                delta_time = now - last_report

                mbps = delta_bytes / (1024 * 1024) / delta_time
                gbps = mbps * 8 / 1000

                print(
                    f"[{now-start:5.1f}s] "
                    f"{mbps:8.2f} MB/s "
                    f"({gbps:.2f} Gbps) "
                    f"{delta_writes:6d} writes/s"
                )

                last_report = now
                last_bytes = bytes_sent
                last_writes = writes

    finally:
        stop_event.set()
        await asyncio.gather(receiver, return_exceptions=True)

        try:
            await stream.close()
        except Exception:
            pass

    elapsed = time.perf_counter() - start

    sent_mb = bytes_sent / (1024 * 1024)
    recv_mb = stats["recv_bytes"] / (1024 * 1024)

    print("\n============== RESULTS =================")
    print(f"Elapsed Time        : {elapsed:.3f} s")
    print(f"Chunk Size          : {chunk_size:,} bytes")
    print(f"Writes              : {writes:,}")
    print(f"Writes/sec          : {writes / elapsed:.2f}")
    print(f"Bytes Sent          : {bytes_sent:,}")
    print(f"Bytes Received      : {stats['recv_bytes']:,}")
    print(f"Data Sent           : {sent_mb:.2f} MB")
    print(f"Data Received       : {recv_mb:.2f} MB")

    throughput = sent_mb / elapsed
    print(f"Throughput          : {throughput:.2f} MB/s")
    print(f"Throughput          : {throughput * 8 / 1000:.2f} Gbps")

    if writes:
        print(f"Average write_all() : {total_write_ns / writes / 1000:.2f} µs")
        print(f"Maximum write_all() : {max_write_ns / 1000:.2f} µs")

    print("========================================")


async def main():
    parser = argparse.ArgumentParser(
        description="WebTransport Upload Benchmark"
    )

    parser.add_argument(
        "--url",
        default="https://127.0.0.1:4433/echo",
        help="WebTransport URL",
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Benchmark duration (seconds)",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=64 * 1024,
        help="Chunk size in bytes",
    )

    args = parser.parse_args()

    config = ClientConfig(
        verify_mode=ssl.CERT_NONE
    )

    print("Connecting...")

    connect_start = time.perf_counter()

    async with WebTransportClient(config=config) as client:
        session = await client.connect(args.url)

        connect_time = time.perf_counter() - connect_start

        print(f"Connected in {connect_time:.3f} s")

        await benchmark(
            session=session,
            chunk_size=args.chunk_size,
            duration=args.duration,
        )


if __name__ == "__main__":
    try:
        import uvloop

        asyncio.set_event_loop_policy(
            uvloop.EventLoopPolicy()
        )
        print("Using uvloop")
    except ImportError:
        print("Using default asyncio")

    asyncio.run(main())
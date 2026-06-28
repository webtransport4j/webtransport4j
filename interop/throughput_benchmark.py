import asyncio
import ssl
import logging
import time

from pywebtransport import ClientConfig, WebTransportClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ThroughputBenchmark")


async def run_stream_throughput_test(client, url, duration_secs=5.0):
    """Measures bidirectional stream throughput (MB/s)."""
    logger.info("🧪 --- Running Bidirectional Stream Throughput Test ---")
    session = await client.connect(url=url)
    stream = await session.create_bidirectional_stream()

    # Init echo handshake
    await stream.write_all(data=b"INIT_BENCHMARK", end_stream=False)
    init_ack = await asyncio.wait_for(stream.read(), timeout=5.0)
    assert b"ACK BI: INIT_BENCHMARK" in init_ack
    logger.info("✅ Echo channel initialized. Blasting for %ds...", duration_secs)

    chunk_size = 64 * 1024  # 64 KB
    payload = b"\x42" * chunk_size
    total_sent = 0
    total_recv = 0
    done = False

    async def reader():
        nonlocal total_recv
        try:
            while not done:
                chunk = await asyncio.wait_for(stream.read(), timeout=1.0)
                if not chunk:
                    break
                total_recv += len(chunk)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        except Exception:
            pass

    read_task = asyncio.create_task(reader())
    t0 = time.monotonic()
    end = t0 + duration_secs

    try:
        while time.monotonic() < end:
            await stream.write_all(data=payload, end_stream=False)
            total_sent += chunk_size
    except Exception as e:
        logger.warning("Write stopped: %s", e)

    done = True
    await asyncio.sleep(0.5)
    read_task.cancel()
    try:
        await read_task
    except asyncio.CancelledError:
        pass

    dt = time.monotonic() - t0 - 0.5
    if dt <= 0:
        dt = duration_secs

    ms = total_sent / (1024 * 1024)
    mr = total_recv / (1024 * 1024)
    logger.info("📊 --- STREAM THROUGHPUT (%.2fs) ---", dt)
    logger.info("📤 Sent:     %.2f MB  (%.2f MB/s  |  %.3f Gbps)", ms, ms / dt, ms / dt * 8 / 1000)
    logger.info("📥 Received: %.2f MB  (%.2f MB/s  |  %.3f Gbps)", mr, mr / dt, mr / dt * 8 / 1000)
    logger.info("=" * 60)

    try:
        await session.close()
    except Exception:
        pass


async def run_datagram_ops_test(client, url, duration_secs=3.0):
    """Measures datagram send throughput (ops/sec)."""
    logger.info("🧪 --- Running Datagram Ops/Sec Test ---")
    session = await client.connect(url=url)

    total = 0
    payload = b"BenchDG"
    t0 = time.monotonic()
    end = t0 + duration_secs

    try:
        while time.monotonic() < end:
            await session.send_datagram(data=payload)
            total += 1
    except Exception as e:
        logger.warning("Datagram stopped: %s", e)

    dt = time.monotonic() - t0
    logger.info("📊 --- DATAGRAM OPS (%.2fs) ---", dt)
    logger.info("📤 Sent: %d datagrams  (%.2f ops/sec)", total, total / dt)
    logger.info("=" * 60)

    try:
        await session.close()
    except Exception:
        pass


async def run_latency_test(client, url, num_streams=16, msgs=50):
    """Measures round-trip latency across concurrent streams."""
    logger.info("🧪 --- Running Latency Test (%d streams × %d msgs) ---", num_streams, msgs)
    session = await client.connect(url=url)
    session.create_bidirectional_stream()
    session.create_unidirectional_stream()
    latencies = []

    async def worker(idx):
        lats = []
        stream = await session.create_bidirectional_stream()
        for i in range(msgs):
            msg = f"LAT_{idx}_{i}".encode()
            t0 = time.monotonic()
            await stream.write_all(data=msg, end_stream=False)
            await asyncio.wait_for(stream.read(), timeout=5.0)
            lats.append((time.monotonic() - t0) * 1000)
        await stream.write_all(data=b"", end_stream=True)
        return lats

    results = await asyncio.gather(*[worker(i) for i in range(num_streams)])
    for r in results:
        latencies.extend(r)

    latencies.sort()
    n = len(latencies)
    avg = sum(latencies) / n
    p50 = latencies[int(n * 0.50)]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]

    logger.info("📊 --- LATENCY RESULTS (%d round-trips) ---", n)
    logger.info("⏱  Avg: %.3f ms | p50: %.3f ms | p95: %.3f ms | p99: %.3f ms", avg, p50, p95, p99)
    logger.info("⏱  Min: %.3f ms | Max: %.3f ms", latencies[0], latencies[-1])
    logger.info("📈 Per-stream throughput: %.0f ops/sec", 1000 / avg)
    logger.info("=" * 60)

    try:
        await session.close()
    except Exception:
        pass


async def main():
    url = "https://localhost:4433/test"
    config = ClientConfig(
        verify_mode=ssl.CERT_NONE,
        log_level="DEBUG",

        # WebTransport flow control
        initial_max_data=64 * 1024 * 1024 * 1024 * 1024,
        initial_max_streams_bidi=100,
        initial_max_streams_uni=100,

        # QUIC transport
        quic_max_concurrent_bidi_streams=100,
        quic_max_concurrent_uni_streams=100,
        quic_receive_window=64 * 1024 * 1024 * 1024 * 1024,
        quic_send_window=64 * 1024 * 1024 * 1024 * 1024,
        quic_stream_receive_window=64 * 1024 * 1024 * 1024 * 1024,

        # Buffers
        max_stream_read_buffer_size=64 * 1024 * 1024 * 1024 * 1024,
        max_stream_write_buffer_size=64 * 1024 * 1024 * 1024 * 1024,

        # Datagram
        max_datagram_size=65527,
    )

    logger.info("=" * 60)
    logger.info("🚀 WebTransport Throughput Benchmark 🚀")
    logger.info("=" * 60)

    async with WebTransportClient(config=config) as client:
        try:
            await run_stream_throughput_test(client, url, duration_secs=5.0)
            print()
            await run_datagram_ops_test(client, url, duration_secs=3.0)
            print()
            await run_latency_test(client, url, num_streams=16, msgs=50)
            print()
            logger.info("🎉 ALL BENCHMARKS COMPLETED!")
        except Exception as e:
            logger.error("Benchmark failed: %s", e, exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())

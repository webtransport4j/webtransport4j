import asyncio
import ssl
import logging
import time

try:
    import uvloop
    # uvloop makes asyncio 2-4x faster, essential for high-stress networking in Python
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    HAS_UVLOOP = True
except ImportError:
    HAS_UVLOOP = False

from pywebtransport import ClientConfig, WebTransportClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("StressBenchmark")

async def run_stream_throughput_test(client, url, duration_secs=5.0, num_streams=8):
    """Measures bidirectional stream throughput across MULTIPLE concurrent streams."""
    logger.info("🧪 --- Running Multi-Stream Throughput Stress Test (%d streams) ---", num_streams)
    session = await client.connect(url=url)

    chunk_size = 64 * 1024  # 64 KB chunks
    payload = b"\x42" * chunk_size
    done = False
    
    total_sent = 0
    total_recv = 0

    async def stream_worker(stream_id):
        nonlocal total_sent, total_recv
        stream = await session.create_bidirectional_stream()
        
        # Init echo handshake
        await stream.write_all(data=f"INIT_BENCHMARK_{stream_id}".encode(), end_stream=False)
        try:
            init_ack = await asyncio.wait_for(stream.read(), timeout=5.0)
            if b"ACK BI:" not in init_ack:
                logger.debug("Stream %d handshake unexpected: %s", stream_id, init_ack)
        except Exception as e:
            logger.warning("Handshake failed on stream %d: %s", stream_id, e)
            return

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
        
        try:
            batch_size = 50  # Increased batch size to reduce context switching overhead
            while not done:
                for _ in range(batch_size):
                    await stream.write_all(data=payload, end_stream=False)
                    total_sent += chunk_size
                await asyncio.sleep(0) # Yield back to event loop
        except Exception as e:
            logger.debug("Write stopped on stream %d: %s", stream_id, e)

        read_task.cancel()

    # Launch all streams concurrently
    t0 = time.monotonic()
    workers = [asyncio.create_task(stream_worker(i)) for i in range(num_streams)]
    
    logger.info("✅ Blasting streams for %ds...", duration_secs)
    await asyncio.sleep(duration_secs)
    done = True
    
    # Wait for tasks to clean up
    await asyncio.gather(*workers, return_exceptions=True)

    dt = time.monotonic() - t0
    ms = total_sent / (1024 * 1024)
    mr = total_recv / (1024 * 1024)
    
    logger.info("📊 --- STREAM THROUGHPUT RESULTS (%.2fs) ---", dt)
    logger.info("📤 Sent:     %.2f MB  (%.2f MB/s  |  %.3f Gbps)", ms, ms / dt, ms / dt * 8 / 1000)
    logger.info("📥 Received: %.2f MB  (%.2f MB/s  |  %.3f Gbps)", mr, mr / dt, mr / dt * 8 / 1000)
    logger.info("=" * 60)

    try:
        await session.close()
    except Exception:
        pass


async def run_datagram_ops_test(client, url, duration_secs=3.0, concurrency=10):
    """Measures datagram send throughput using highly concurrent workers."""
    logger.info("🧪 --- Running Datagram Ops/Sec Stress Test (%d workers) ---", concurrency)
    session = await client.connect(url=url)

    total = 0
    payload = b"BenchDG_Stress"
    done = False

    async def worker():
        nonlocal total
        batch = 100
        try:
            while not done:
                for _ in range(batch):
                    await session.send_datagram(data=payload)
                    total += 1
                await asyncio.sleep(0) # Yield for other workers
        except Exception as e:
            logger.debug("Worker stopped: %s", e)

    t0 = time.monotonic()
    tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
    
    await asyncio.sleep(duration_secs)
    done = True
    await asyncio.gather(*tasks, return_exceptions=True)

    dt = time.monotonic() - t0
    logger.info("📊 --- DATAGRAM OPS RESULTS (%.2fs) ---", dt)
    logger.info("📤 Sent: %d datagrams  (%.2f ops/sec)", total, total / dt)
    logger.info("=" * 60)

    try:
        await session.close()
    except Exception:
        pass


async def run_latency_test(client, url, num_streams=50, msgs=50):
    """Measures round-trip latency across high concurrent streams to measure queuing stress."""
    logger.info("🧪 --- Running Latency Stress Test (%d streams × %d msgs) ---", num_streams, msgs)
    session = await client.connect(url=url)
    latencies = []

    async def worker(idx):
        lats = []
        try:
            stream = await session.create_bidirectional_stream()
            for i in range(msgs):
                msg = f"LAT_{idx}_{i}".encode()
                t0 = time.monotonic()
                await stream.write_all(data=msg, end_stream=False)
                await asyncio.wait_for(stream.read(), timeout=5.0)
                lats.append((time.monotonic() - t0) * 1000)
            await stream.write_all(data=b"", end_stream=True)
        except Exception as e:
            logger.debug("Latency stream %d failed: %s", idx, e)
        return lats

    results = await asyncio.gather(*[worker(i) for i in range(num_streams)])
    for r in results:
        latencies.extend(r)

    if not latencies:
        logger.error("❌ No latency results gathered. Check server.")
        return

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
    url = "https://127.0.0.1:4433/test"
    import inspect
    sig = inspect.signature(ClientConfig)
    params = sig.parameters

    config_args = {
        "verify_mode": ssl.CERT_NONE,
        "log_level": "WARNING", # Reduced from DEBUG to avoid I/O logging bottleneck

        # WebTransport flow control (Maximized for stress testing)
        "initial_max_data": 2147483647,
        "initial_max_streams_bidi": 500,
        "initial_max_streams_uni": 500,
        "max_datagram_size": 65527,
    }

    if "initial_max_stream_data_bidi_local" in params:
        config_args["initial_max_stream_data_bidi_local"] = 68719476735
        config_args["initial_max_stream_data_uni"] = 68719476735
    else:
        # Massive windows for high throughput stress
        window_size = 500 * 1024 * 1024
        config_args["quic_max_concurrent_bidi_streams"] = 500
        config_args["quic_max_concurrent_uni_streams"] = 500
        config_args["quic_receive_window"] = window_size
        config_args["quic_send_window"] = window_size
        config_args["quic_stream_receive_window"] = window_size
        config_args["max_stream_read_buffer_size"] = window_size
        config_args["max_stream_write_buffer_size"] = window_size

    config = ClientConfig(**config_args)

    logger.info("=" * 60)
    logger.info("🚀 WebTransport HIGH STRESS Benchmark 🚀")
    logger.info("⚡ uvloop accelerated: %s", "YES" if HAS_UVLOOP else "NO (install uvloop for better results)")
    logger.info("=" * 60)

    import sys
    test_num = None
    if len(sys.argv) > 1:
        try:
            test_num = int(sys.argv[1])
            logger.info("🎯 Running Test Option: #%d", test_num)
        except ValueError:
            logger.info("⚠️ Invalid test option, running all tests.")

    async with WebTransportClient(config=config) as client:
        try:
            if test_num is None or test_num == 1:
                # 8 concurrent streams for throughput stress
                await run_stream_throughput_test(client, url, duration_secs=6.0, num_streams=8)
                print()
            if test_num is None or test_num == 2:
                # 20 concurrent workers blasting datagrams
                await run_datagram_ops_test(client, url, duration_secs=6.0, concurrency=20)
                print()
            if test_num is None or test_num == 3:
                # 100 concurrent streams doing ping-pongs
                await run_latency_test(client, url, num_streams=100, msgs=50)
                print()
            logger.info("🎉 STRESS BENCHMARK COMPLETED!")
        except Exception as e:
            logger.error("Benchmark failed: %s", e, exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
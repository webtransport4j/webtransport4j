import asyncio
import ssl
import logging
import time
import argparse
import inspect
import sys

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

    try:
        session = await client.connect(url=url)
    except Exception as e:
        logger.error("Failed to connect: %s", e)
        return

    try:
        chunk_size = 64 * 1024  # 64 KB chunks
        payload = b"\x42" * chunk_size
        done = asyncio.Event()

        total_sent = 0
        total_recv = 0

        async def stream_worker(stream_id):
            nonlocal total_sent, total_recv
            try:
                stream = await session.create_bidirectional_stream()

                # Init echo handshake
                await stream.write_all(data=f"INIT_BENCHMARK_{stream_id}".encode(), end_stream=False)
                init_ack = await asyncio.wait_for(stream.read(), timeout=5.0)

                if b"ACK BI:" not in init_ack:
                    logger.debug("Stream %d handshake unexpected: %s", stream_id, init_ack)

            except asyncio.TimeoutError:
                logger.warning("Handshake timed out on stream %d", stream_id)
                return
            except Exception as e:
                logger.warning("Handshake failed on stream %d: %s", stream_id, e)
                return

            async def reader():
                nonlocal total_recv
                try:
                    while not done.is_set():
                        chunk = await asyncio.wait_for(stream.read(), timeout=1.0)
                        if not chunk:
                            break
                        total_recv += len(chunk)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
                except Exception as e:
                    logger.debug("Reader error on stream %d: %s", stream_id, e)

            read_task = asyncio.create_task(reader())

            try:
                batch_size = 50
                while not done.is_set():
                    for _ in range(batch_size):
                        await stream.write_all(data=payload, end_stream=False)
                        total_sent += chunk_size
                    await asyncio.sleep(0) # Yield back to event loop
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug("Write stopped on stream %d: %s", stream_id, e)
            finally:
                read_task.cancel()
                try:
                    await read_task
                except asyncio.CancelledError:
                    pass

        # Launch all streams concurrently
        t0 = time.perf_counter()
        workers = [asyncio.create_task(stream_worker(i)) for i in range(num_streams)]

        logger.info("✅ Blasting streams for %ds...", duration_secs)
        await asyncio.sleep(duration_secs)
        done.set()

        # Wait for tasks to clean up safely
        await asyncio.gather(*workers, return_exceptions=True)

        dt = time.perf_counter() - t0
        ms = total_sent / (1024 * 1024)
        mr = total_recv / (1024 * 1024)

        logger.info("📊 --- STREAM THROUGHPUT RESULTS (%.2fs) ---", dt)
        logger.info("📤 Sent:     %.2f MB  (%.2f MB/s  |  %.3f Gbps)", ms, ms / dt, ms / dt * 8 / 1000)
        logger.info("📥 Received: %.2f MB  (%.2f MB/s  |  %.3f Gbps)", mr, mr / dt, mr / dt * 8 / 1000)
        logger.info("=" * 60)

    finally:
        try:
            await session.close()
        except Exception as e:
            logger.debug("Error closing session cleanly: %s", e)


async def run_single_stream_ops_test(client, url, duration_secs=3.0):
    """Measures ops/sec by pipelining messages over EXACTLY ONE persistent stream."""
    logger.info("🧪 --- Running Single-Stream Ops/Sec Stress Test ---")

    try:
        session = await client.connect(url=url)
    except Exception as e:
        logger.error("Failed to connect: %s", e)
        return

    try:
        # Open exactly ONE bidirectional stream
        stream = await session.create_bidirectional_stream()
        payload = b"Bench_Ping\n"

        done = asyncio.Event()
        total_sent = 0
        total_recv_ops = 0

        async def writer():
            nonlocal total_sent
            batch = 100
            try:
                while not done.is_set():
                    for _ in range(batch):
                        # Send payload without closing the stream
                        await stream.write_all(data=payload, end_stream=False)
                        total_sent += 1
                    # Yield to the event loop so the reader can process acks
                    await asyncio.sleep(0)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug("Writer stopped: %s", e)

        async def reader():
            nonlocal total_recv_ops
            try:
                while not done.is_set():
                    # Read incoming acknowledgments from the SAME stream
                    chunk = await asyncio.wait_for(stream.read(), timeout=1.0)
                    if not chunk:
                        break

                    # Estimate acks received (Assuming server echoes back similar size)
                    total_recv_ops += max(1, len(chunk) // len(payload))
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except Exception as e:
                logger.debug("Reader stopped: %s", e)

        # Start blasting and reading simultaneously on the single stream
        t0 = time.perf_counter()
        writer_task = asyncio.create_task(writer())
        reader_task = asyncio.create_task(reader())

        logger.info("✅ Blasting single stream for %ds...", duration_secs)
        await asyncio.sleep(duration_secs)
        done.set()

        # Cleanly cancel and wait for tasks to finish
        writer_task.cancel()
        reader_task.cancel()
        await asyncio.gather(writer_task, reader_task, return_exceptions=True)

        # Signal to the server that we are done with this stream
        try:
            await stream.write_all(b"", end_stream=True)
        except Exception:
            pass

        dt = time.perf_counter() - t0
        logger.info("📊 --- SINGLE STREAM PIPELINE RESULTS (%.2fs) ---", dt)
        logger.info("📤 Sent:     %d ops (%.2f ops/sec)", total_sent, total_sent / dt)
        logger.info("📥 Received: ~%d acks (%.2f acks/sec)", total_recv_ops, total_recv_ops / dt)
        logger.info("=" * 60)

    finally:
        try:
            await session.close()
        except Exception as e:
            logger.debug("Error closing session: %s", e)


async def run_latency_test(client, url, num_streams=50, msgs=50):
    """Measures round-trip latency across high concurrent streams to measure queuing stress."""
    logger.info("🧪 --- Running Latency Stress Test (%d streams × %d msgs) ---", num_streams, msgs)

    try:
        session = await client.connect(url=url)
    except Exception as e:
        logger.error("Failed to connect: %s", e)
        return

    try:
        latencies = []

        async def worker(idx):
            lats = []
            try:
                stream = await session.create_bidirectional_stream()
                for i in range(msgs):
                    msg = f"LAT_{idx}_{i}".encode()
                    t0 = time.perf_counter()
                    await stream.write_all(data=msg, end_stream=False)

                    # Guard against stalls
                    await asyncio.wait_for(stream.read(), timeout=5.0)
                    lats.append((time.perf_counter() - t0) * 1000)

                await stream.write_all(data=b"", end_stream=True)
            except asyncio.TimeoutError:
                logger.debug("Latency stream %d timed out.", idx)
            except Exception as e:
                logger.debug("Latency stream %d failed: %s", idx, e)
            return lats

        results = await asyncio.gather(*[worker(i) for i in range(num_streams)], return_exceptions=True)

        for r in results:
            if isinstance(r, list):
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

    finally:
        try:
            await session.close()
        except Exception as e:
            logger.debug("Error closing session cleanly: %s", e)


def build_config() -> ClientConfig:
    """Builds the WebTransport Client configuration dynamically based on available library kwargs."""
    sig = inspect.signature(ClientConfig)
    params = sig.parameters

    config_args = {
        "verify_mode": ssl.CERT_NONE,
        "log_level": "WARNING", # Reduced to avoid I/O logging bottleneck
        "initial_max_data": 2147483647,
        "initial_max_streams_bidi": 500,
        "initial_max_streams_uni": 500,
        "max_datagram_size": 65527,
    }

    # Safe fallback configuration injection
    if "initial_max_stream_data_bidi_local" in params:
        config_args["initial_max_stream_data_bidi_local"] = 68719476735
        config_args["initial_max_stream_data_uni"] = 68719476735
    else:
        window_size = 500 * 1024 * 1024
        fallback_params = {
            "quic_max_concurrent_bidi_streams": 500,
            "quic_max_concurrent_uni_streams": 500,
            "quic_receive_window": window_size,
            "quic_send_window": window_size,
            "quic_stream_receive_window": window_size,
            "max_stream_read_buffer_size": window_size,
            "max_stream_write_buffer_size": window_size
        }
        for k, v in fallback_params.items():
            if k in params:
                config_args[k] = v

    return ClientConfig(**config_args)


async def main():
    parser = argparse.ArgumentParser(description="High-Stress WebTransport Benchmark tool.")
    parser.add_argument("--url", type=str, default="https://127.0.0.1:4433/test", help="Target WebTransport URL.")
    parser.add_argument("--test", type=int, choices=[1, 2, 3], help="Specific test (1: Throughput, 2: Single-Stream Pipeline, 3: Latency). If omitted, runs all.")
    parser.add_argument("--duration", type=float, default=1.0, help="Duration in seconds for throughput and ops tests.")
    args = parser.parse_args()

    config = build_config()

    logger.info("=" * 60)
    logger.info("🚀 WebTransport HIGH STRESS Benchmark 🚀")
    logger.info("🎯 Target: %s", args.url)
    logger.info("⚡ uvloop accelerated: %s", "YES" if HAS_UVLOOP else "NO (install uvloop for better results)")
    logger.info("=" * 60)

    try:
        async with WebTransportClient(config=config) as client:

            if args.test is None or args.test == 1:
                await run_stream_throughput_test(client, args.url, duration_secs=args.duration, num_streams=8)
                print()

            if args.test is None or args.test == 2:
                # Runs the new single-stream ops test instead of datagrams
                await run_single_stream_ops_test(client, args.url, duration_secs=args.duration)
                print()

            if args.test is None or args.test == 3:
                await run_latency_test(client, args.url, num_streams=100, msgs=50)
                print()

            logger.info("🎉 STRESS BENCHMARK COMPLETED!")

    except KeyboardInterrupt:
        logger.info("🛑 Benchmark cleanly interrupted by user.")
    except Exception as e:
        logger.critical("Benchmark failed catastrophically: %s", e, exc_info=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Catching it here as well ensures quiet exit if interrupted during event loop setup/teardown
        sys.exit(0)
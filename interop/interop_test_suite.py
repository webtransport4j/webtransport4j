import asyncio
import ssl
import logging
import time

from pywebtransport import ClientConfig, WebTransportClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("WebTransportTestSuite")

pending_verifications = set()

async def test_datagrams(session):
    logger.info("🧪 --- Running Datagram Test ---")
    payload_id = f"PingDatagram_{time.time()}"
    pending_verifications.add(payload_id)
    
    test_msg = payload_id.encode('utf-8')
    await session.send_datagram(data=test_msg)
    logger.info(f"✅ Datagram {payload_id} Sent (Waiting for ACK on Uni Stream...)")
    
    # Wait for echo since we updated Java side to echo via a NEW Unidirectional Stream
    try:
        ack_stream = await asyncio.wait_for(session.accept_unidirectional_stream(), timeout=5.0)
        response = await asyncio.wait_for(ack_stream.read(), timeout=3.0)
        response_str = response.decode('utf-8', errors='replace')
        logger.info(f"Received Datagram ACK on Uni Stream: {response_str}")
        assert f"ACK DG: {payload_id}" in response_str
        pending_verifications.remove(payload_id)
        logger.info("✅ Datagram Test Passed (ACK received via Uni Stream)")
    except asyncio.TimeoutError:
        logger.error("❌ Datagram Test Failed: Timeout waiting for echo")
        raise

async def test_client_bidi_stream(session):
    logger.info("🧪 --- Running Client Bidi Stream Test ---")
    stream = await session.create_bidirectional_stream()
    payload_id = f"PingBidi_{time.time()}"
    pending_verifications.add(payload_id)
    
    test_msg = payload_id.encode('utf-8')
    await stream.write_all(data=test_msg, end_stream=False)
    
    # Wait for echo
    try:
        response = await asyncio.wait_for(stream.read(), timeout=3.0)
        response_str = response.decode('utf-8', errors='replace')
        logger.info(f"Received from Bidi: {response_str}")
        assert f"ACK BI: {payload_id}" in response_str
        pending_verifications.remove(payload_id)
        
        # Clean up
        await stream.write_all(data=b"", end_stream=True)
        logger.info("✅ Client Bidi Stream Test Passed")
    except asyncio.TimeoutError:
        logger.error("❌ Client Bidi Stream Test Failed: Timeout waiting for echo")
        raise

async def test_client_uni_stream(session):
    logger.info("🧪 --- Running Client Uni Stream Test ---")
    stream = await session.create_unidirectional_stream()
    payload_id = f"PingUni_{time.time()}"
    pending_verifications.add(payload_id)
    
    test_msg = payload_id.encode('utf-8')
    await stream.write_all(data=test_msg, end_stream=True)
    
    # Wait for the out-of-band ACK sent by the server via a NEW Unidirectional Stream
    try:
        ack_stream = await asyncio.wait_for(session.accept_unidirectional_stream(), timeout=5.0)
        response = await asyncio.wait_for(ack_stream.read(), timeout=3.0)
        response_str = response.decode('utf-8', errors='replace')
        logger.info(f"Received ACK on new Uni Stream: {response_str}")
        assert f"ACK UNI: {payload_id}" in response_str
        pending_verifications.remove(payload_id)
        logger.info("✅ Client Uni Stream Test Passed (Strict ACK asserted)")
    except asyncio.TimeoutError:
        logger.error("❌ Client Uni Stream Test Failed: Timeout waiting for ACK stream")
        raise

async def handle_server_streams(session):
    """
    Background task to catch the server-initiated streams that WebTransportTestHandler
    fires off immediately in onSessionReady().
    """
    logger.info("🎧 Listening for Server-initiated streams...")
    pending_verifications.add("Server_Uni_Received")
    pending_verifications.add("Server_Bidi_Received")
    
    try:
        uni_stream = await session.accept_unidirectional_stream()
        logger.info(f"📥 Accepted Server Uni Stream ID: {uni_stream.stream_id}")
        uni_data = await asyncio.wait_for(uni_stream.read(), timeout=5.0)
        logger.info(f"Server Uni Data (Snippet): {uni_data[:100]}...")
        assert b"Hello from Server-Initiated Unidirectional Stream!" in uni_data, "Server Uni data content mismatch"
        pending_verifications.remove("Server_Uni_Received")
        
        bidi_stream = await session.accept_bidirectional_stream()
        logger.info(f"📥 Accepted Server Bidi Stream ID: {bidi_stream.stream_id}")
        bidi_data = await asyncio.wait_for(bidi_stream.read(), timeout=5.0)
        logger.info(f"Server Bidi Data (Snippet): {bidi_data[:100]}...")
        assert b"Hello from Server-Initiated Bidirectional Stream!" in bidi_data, "Server Bidi data content mismatch"
        pending_verifications.remove("Server_Bidi_Received")
        
        # Send a reply back on the Bidi stream
        await bidi_stream.write_all(data=b"ACK SERVER BIDI: Greetings from Python", end_stream=True)
        logger.info("✅ Server Streams Received, Verified, and Handled.")
    except asyncio.TimeoutError:
        logger.warning("⚠️ Timeout waiting for server-initiated streams. Is the server configured to send them?")
    except Exception as e:
        logger.error(f"❌ Error handling server streams: {e}")

async def test_large_payload(session):
    logger.info("🧪 --- Running Large Payload Test (Data Integrity) ---")
    stream = await session.create_bidirectional_stream()
    payload_id = f"LargePayload_{time.time()}"
    pending_verifications.add(payload_id)
    
    # 250KB payload
    chunk = b"0123456789ABCDEF" * 16384
    test_msg = payload_id.encode('utf-8') + b"_" + chunk
    
    await stream.write_all(data=test_msg, end_stream=False)
    
    try:
        received_data = bytearray()
        expected_len = len(b"ACK BI: ") + len(test_msg)
        while len(received_data) < expected_len:
            data = await asyncio.wait_for(stream.read(), timeout=5.0)
            if not data:
                break
            received_data.extend(data)
            
        assert (b"ACK BI: " + test_msg) in received_data, "Large payload corrupted!"
        pending_verifications.remove(payload_id)
        
        await stream.write_all(data=b"", end_stream=True)
        logger.info("✅ Large Payload Test Passed")
    except Exception as e:
        logger.error(f"❌ Large Payload Test Failed: {e}")
        raise

async def test_concurrent_streams(session):
    logger.info("🧪 --- Running Concurrent Streams Stress Test ---")
    num_streams = 8
    tasks = []
    
    async def run_single_stream(idx):
        stream = await session.create_bidirectional_stream()
        payload_id = f"Concurrent_{idx}_{time.time()}"
        pending_verifications.add(payload_id)
        
        await stream.write_all(data=payload_id.encode('utf-8'), end_stream=False)
        response = await asyncio.wait_for(stream.read(), timeout=5.0)
        response_str = response.decode('utf-8', errors='replace')
        assert f"ACK BI: {payload_id}" in response_str, f"Mismatch in concurrent stream {idx}"
        pending_verifications.remove(payload_id)
        await stream.write_all(data=b"", end_stream=True)
    
    for i in range(num_streams):
        tasks.append(run_single_stream(i))
        
    try:
        await asyncio.gather(*tasks)
        logger.info(f"✅ Concurrent Streams Test Passed ({num_streams} streams multiplexed)")
    except Exception as e:
        logger.error(f"❌ Concurrent Streams Test Failed: {e}")
        raise

async def test_no_head_of_line_blocking(session):
    logger.info("🧪 --- Running Application-Level HOLB Test ---")
    
    # 1. Open the Hog stream
    hog_stream = await session.create_bidirectional_stream()
    hog_payload = f"SleepServer_{time.time()}"
    pending_verifications.add(hog_payload)
    
    # Send the sleep command and do NOT await its read yet
    await hog_stream.write_all(data=hog_payload.encode('utf-8'), end_stream=False)
    
    # Wait a tiny bit to ensure the server starts processing the hog stream and goes to sleep
    await asyncio.sleep(0.5)
    
    # 2. Open the Fast stream
    fast_stream = await session.create_bidirectional_stream()
    fast_payload = f"Ping_Fast_{time.time()}"
    pending_verifications.add(fast_payload)
    
    start_time = time.time()
    await fast_stream.write_all(data=fast_payload.encode('utf-8'), end_stream=False)
    
    # 3. Assert the Fast stream gets its response immediately, well before the 3 seconds!
    try:
        fast_response = await asyncio.wait_for(fast_stream.read(), timeout=1.5)
        elapsed = time.time() - start_time
        assert f"ACK BI: {fast_payload}" in fast_response.decode('utf-8', errors='replace')
        logger.info(f"✅ Fast stream completed in {elapsed:.3f}s (bypassing the sleeping Hog stream!)")
        pending_verifications.remove(fast_payload)
        await fast_stream.write_all(data=b"", end_stream=True)
    except TimeoutError:
        logger.error("❌ HOLB Test Failed: Fast stream blocked by Hog stream!")
        raise
        
    # 4. Now await the Hog stream's response (which should take another ~2.5s)
    try:
        hog_response = await asyncio.wait_for(hog_stream.read(), timeout=4.0)
        assert f"ACK BI: {hog_payload}" in hog_response.decode('utf-8', errors='replace')
        logger.info("✅ Hog stream finally completed successfully.")
        pending_verifications.remove(hog_payload)
        await hog_stream.write_all(data=b"", end_stream=True)
    except TimeoutError:
        logger.error("❌ HOLB Test Failed: Hog stream timed out!")
        raise

async def test_stream_flow_control(session):
    logger.info("🧪 --- Running Stream Limit Exhaustion Test ---")
    streams = []
    blocked = False
    
    try:
        # We wrap in asyncio.wait_for to detect the block.
        for i in range(1, 150):
            stream = await asyncio.wait_for(session.create_bidirectional_stream(), timeout=1.0)
            streams.append(stream)
            if i % 50 == 0:
                logger.info(f"Successfully opened {i} streams...")
    except asyncio.TimeoutError:
        logger.info(f"✅ Flow control working! Stream creation blocked after opening {len(streams)} streams.")
        blocked = True
    
    if not blocked:
        logger.warning("⚠️ Flow control test aborted: server limit is > 150. Lower your config to test this.")
    
    # Clean up the streams we opened so we don't leak them
    logger.info("Cleaning up streams...")
    for s in streams:
        try:
            await s.write_all(data=b"", end_stream=True)
        except Exception:
            pass

async def test_heartbeat_idle(session):
    logger.info("🧪 --- Running Heartbeat / Idle Test ---")
    # Wait for 15 seconds. If the server drops us, the next operation will fail.
    logger.info("Sleeping for 15 seconds to verify connection stability...")
    await asyncio.sleep(15)
    
    # Verify connection is still alive by running a quick uni test again and explicitly waiting for the ACK
    stream = await session.create_unidirectional_stream()
    payload_id = f"PingAfterSleep_{time.time()}"
    test_msg = payload_id.encode('utf-8')
    await stream.write_all(data=test_msg, end_stream=True)
    
    ack_stream = await asyncio.wait_for(session.accept_unidirectional_stream(), timeout=5.0)
    response = await asyncio.wait_for(ack_stream.read(), timeout=3.0)
    response_str = response.decode('utf-8', errors='replace')
    assert f"ACK UNI: {payload_id}" in response_str, "Did not receive strict ACK after idle period"
    
    logger.info("✅ Connection survived idle period and successfully exchanged data!")

async def test_heartbeat_timeout_negative(client, url):
    logger.info("🧪 --- Running Heartbeat Timeout Negative Test ---")
    logger.info("🔗 Establishing a separate connection to test timeout drops...")
    session = await client.connect(url=url)
    logger.info("✅ Connection Established! Now sleeping for 36 seconds (exceeding 30s server limit)...")
    await asyncio.sleep(36)
    
    logger.info("Awake! Attempting to use the session. This should FAIL because the server dropped us.")
    dropped = False
    try:
        stream = await asyncio.wait_for(session.create_unidirectional_stream(), timeout=3.0)
        await stream.write_all(data=b"Should not reach here", end_stream=True)
    except Exception as e:
        logger.info(f"✅ Connection was correctly dropped by server! (Caught Exception: {type(e).__name__})")
        dropped = True
        
    assert dropped, "Negative test failed: Connection was STILL ALIVE after 36s of inactivity!"

async def main():
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
    url = "https://localhost:4433/test"

    logger.info("=========================================")
    logger.info("🚀 WebTransport Integration Test Suite 🚀")
    logger.info("=========================================")

    async with WebTransportClient(config=config) as client:
        try:
            logger.info(f"🔗 Connecting to {url}...")
            session = await client.connect(url=url)
            logger.info("✅ Connection Established!\n")
            
            # Start background listener for server streams
            server_stream_task = asyncio.create_task(handle_server_streams(session))
            
            # Wait a moment for server to send its greeting streams
            await asyncio.sleep(1.0)
            
            # Run sequential tests
            await test_datagrams(session)
            print("")
            await test_client_uni_stream(session)
            print("")
            await test_client_bidi_stream(session)
            print("")
            await test_large_payload(session)
            print("")
            await test_concurrent_streams(session)
            print("")
            await test_no_head_of_line_blocking(session)
            print("")
            await test_stream_flow_control(session)
            print("")
            await test_heartbeat_idle(session)
            print("")
            
            # Ensure background tasks for the primary session completed
            await server_stream_task
            
            logger.info("🚪 Closing primary session.")
            await session.close()
            print("")
            
            # Run the negative timeout test which requires its own session
            await test_heartbeat_timeout_negative(client, url)
            print("")
            
            # Final Assertion
            logger.info(f"Pending Verifications Check: {pending_verifications}")
            assert len(pending_verifications) == 0, f"Some verifications never completed: {pending_verifications}"
            
            logger.info("=========================================")
            logger.info("🎉 ALL TESTS COMPLETED SUCCESSFULLY! 🎉")
            logger.info("=========================================")
            
        except Exception as e:
            logger.error(f"❌ TEST SUITE FAILED: {e}", exc_info=True)
        finally:
            logger.info("🏁 Test suite finished.")

if __name__ == "__main__":
    asyncio.run(main())

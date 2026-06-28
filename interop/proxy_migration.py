import asyncio
import ssl
import logging

from pywebtransport import ClientConfig, WebTransportClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("ProxyMigration")

class ProxyClient(asyncio.DatagramProtocol):
    def __init__(self, proxy_server):
        self.proxy_server = proxy_server
        
    def datagram_received(self, data, addr):
        if self.proxy_server.client_addr and self.proxy_server.client_facing_transport:
            self.proxy_server.client_facing_transport.sendto(data, self.proxy_server.client_addr)

class ProxyServer(asyncio.DatagramProtocol):
    def __init__(self, target_addr, loop):
        self.target_addr = target_addr
        self.loop = loop
        self.client_addr = None
        self.client_facing_transport = None
        self.server_facing_transport = None
        
    def connection_made(self, transport):
        self.client_facing_transport = transport
        # Spin up the outgoing UDP socket that talks to the real server
        asyncio.create_task(self._create_outgoing())
        
    async def _create_outgoing(self):
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ProxyClient(self),
            local_addr=('127.0.0.1', 0)
        )
        self.server_facing_transport = transport
        logger.info(f"🟢 PROXY: Bound outgoing server-facing socket on port {transport.get_extra_info('sockname')[1]}")
        
    def datagram_received(self, data, addr):
        # We assume one client for this test
        self.client_addr = addr
        if self.server_facing_transport:
            self.server_facing_transport.sendto(data, self.target_addr)
            
    async def migrate(self):
        logger.info("🔴 PROXY: Triggering Connection Migration (Simulating NAT Rebinding)...")
        if self.server_facing_transport:
            # Abruptly close the socket sending packets to the server
            self.server_facing_transport.close()
        
        # Create a brand new socket on a random port
        await self._create_outgoing()


async def main():
    loop = asyncio.get_running_loop()
    
    # Start proxy on 4434 pointing to real server on 4433
    proxy = ProxyServer(('127.0.0.1', 4433), loop)
    transport, _ = await loop.create_datagram_endpoint(
        lambda: proxy,
        local_addr=('127.0.0.1', 4434)
    )
    logger.info("🎧 Proxy listening on 127.0.0.1:4434 -> forwarding to 127.0.0.1:4433")

    config = ClientConfig(verify_mode=ssl.CERT_NONE)
    # Connect to the proxy instead of the real server
    url = "https://127.0.0.1:4434/test"

    try:
        async with WebTransportClient(config=config) as client:
            logger.info(f"🔗 Connecting to proxy at {url}...")
            session = await client.connect(url=url)
            logger.info("✅ Connection Established through proxy!")

            stream = await session.create_bidirectional_stream()
            
            # 1. Send data from original port
            msg1 = b"Hello before migration"
            await stream.write_all(data=msg1, end_stream=False)
            logger.info(f"📤 Sent: {msg1!r}")
            reply1 = await stream.read()
            logger.info(f"📥 Received from server: {reply1!r}")

            # 2. Trigger the NAT rebinding simulation
            await proxy.migrate()
            await asyncio.sleep(1)

            # 3. Send data from the new port
            msg2 = b"Hello after migration"
            await stream.write_all(data=msg2, end_stream=False)
            logger.info(f"📤 Sent: {msg2!r}")
            reply2 = await stream.read()
            logger.info(f"📥 Received from server: {reply2!r}")

            await stream.write_all(data=b"", end_stream=True)
            logger.info("🚪 Closing down WebTransport session cleanly.")
            await session.close()
    finally:
        transport.close()
        if proxy.server_facing_transport:
            proxy.server_facing_transport.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest terminated cleanly by operator.")

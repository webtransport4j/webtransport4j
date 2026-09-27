#!/usr/bin/env python3
"""
WebTransport Chat Client — Full Test Suite for WebTransportChatHandler.java

Tests ALL server features:
  1. Control Stream (Bidi, prefix 0x01): JOIN/LEAVE commands
  2. Text Chat Stream (Bidi, prefix 0x02): Send/receive text messages
  3. Voice Stream (Uni, prefix 0x03): Client sends voice chunks, server broadcasts
  4. Datagrams: Typing indicator broadcast (TYPING true/false)
  5. Server-initiated Uni Stream: Receives voice broadcast from server (prefix 0x03)
  6. Multi-user scenario: Two concurrent clients in the same room

Requires: pip install pywebtransport
Server:   Java WebTransportServer running on https://localhost:4433/chat
"""

import asyncio
import ssl
import logging
import time
import struct

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)-18s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ChatTest")

from pywebtransport import ClientConfig, WebTransportClient
from pywebtransport.types import EventType
import aioquic.h3.connection
from aioquic.h3.connection import H3Connection
from aioquic.buffer import Buffer


# ═══════════════════════════════════════════════════════════════════════
# 1. CAPSULE PARSER (WT_MAX_STREAMS / WT_CLOSE_SESSION interceptor)
# ═══════════════════════════════════════════════════════════════════════

def decode_varint(data: bytes, offset: int = 0) -> tuple:
    """Decode a QUIC variable-length integer from raw bytes."""
    if offset >= len(data):
        return -1, offset
    first = data[offset]
    length = 1 << (first >> 6)
    if offset + length > len(data):
        return -1, offset
    val_bytes = data[offset: offset + length]
    if length == 1:
        val = val_bytes[0] & 0x3F
    elif length == 2:
        val = int.from_bytes(val_bytes, "big") & 0x3FFF
    elif length == 4:
        val = int.from_bytes(val_bytes, "big") & 0x3FFFFFFF
    else:
        val = int.from_bytes(val_bytes, "big") & 0x3FFFFFFFFFFFFFFF
    return val, offset + length


CAPSULE_NAMES = {
    0x190B4D3F: "WT_MAX_STREAMS (Bidirectional)",
    0x190B4D40: "WT_MAX_STREAMS (Unidirectional)",
    0x190B4D3D: "WT_MAX_DATA",
    0x190B4D41: "WT_DATA_BLOCKED",
    0x190B4D44: "WT_STREAMS_BLOCKED (Bidirectional)",
    0x190B4D45: "WT_STREAMS_BLOCKED (Unidirectional)",
    0x2843:     "CLOSE_WEBTRANSPORT_SESSION",
}


def parse_capsule(data: bytes):
    """Parse capsule bytes and log them."""
    try:
        offset = 0
        while offset < len(data):
            cap_type, offset = decode_varint(data, offset)
            if cap_type == -1:
                break
            cap_len, offset = decode_varint(data, offset)
            if cap_len == -1 or offset + cap_len > len(data):
                break
            cap_val_bytes = data[offset: offset + cap_len]
            offset += cap_len

            type_str = CAPSULE_NAMES.get(cap_type, f"0x{cap_type:X}")
            val, _ = decode_varint(cap_val_bytes) if cap_val_bytes else (-1, 0)
            val_str = str(val) if val != -1 else cap_val_bytes.hex()

            logger.debug(f"💊 [CAPSULE] Type: {type_str} | Len: {cap_len} | Value: {val_str}")
    except Exception as e:
        logger.error(f"❌ Capsule parse error: {e}")


# ─── Monkey-patch aioquic to intercept capsule frames ────────────────
_original_handle_event = H3Connection.handle_event

def patched_handle_event(self, event):
    if type(event).__name__ == "StreamDataReceived":
        try:
            buf = Buffer(data=event.data)
            while not buf.eof():
                frame_type = buf.pull_uint_var()
                frame_length = buf.pull_uint_var()
                if frame_type == 0x4752 and frame_length <= buf.bytes_remaining():
                    capsule_bytes = buf.data[buf.tell():buf.tell() + frame_length]
                    parse_capsule(capsule_bytes)
                buf.seek(buf.tell() + frame_length)
        except Exception:
            pass
    return _original_handle_event(self, event)

aioquic.h3.connection.H3Connection.handle_event = patched_handle_event


# ═══════════════════════════════════════════════════════════════════════
# 2. PROTOCOL CONSTANTS (must match WebTransportChatHandler.java)
# ═══════════════════════════════════════════════════════════════════════
STREAM_TYPE_CONTROL = 0x01
STREAM_TYPE_CHAT    = 0x02
STREAM_TYPE_VOICE   = 0x03


# ═══════════════════════════════════════════════════════════════════════
# 3. HELPER: Receive Loops
# ═══════════════════════════════════════════════════════════════════════

async def read_stream_forever(stream, label: str, received: list, quiet: bool = False):
    """Drain data from a stream, log it, and append to received list."""
    try:
        while True:
            chunk = await stream.read()
            if not chunk:
                if not quiet:
                    logger.info(f"🛑 [{label}] Stream ended (ID: {stream.stream_id})")
                break
            text = chunk.decode("utf-8", errors="replace")
            if not quiet:
                logger.info(f"📥 [{label}] ← {text!r}")
            received.append(text)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        if not quiet:
            logger.error(f"❌ [{label}] read error: {e}")


async def listen_datagrams(session, label: str, received: list):
    """Receive datagrams continuously."""
    try:
        while True:
            event = await session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED)
            if isinstance(event.data, dict) and (d := event.data.get("data")):
                text = d.decode("utf-8", errors="replace") if isinstance(d, bytes) else str(d)
                logger.info(f"📥 [{label} DATAGRAM] ← {text!r}")
                received.append(text)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"❌ [{label} DATAGRAM] error: {e}")


async def listen_server_uni_streams(session, label: str, received: list, quiet_push: bool = False):
    """Listen for server-initiated unidirectional streams.
       Auto-detects voice streams (0x03 prefix) vs test push streams.
       Voice streams are ALWAYS logged. Test push streams are silenced when quiet_push=True."""
    try:
        async for stream in session.incoming_unidirectional_streams():
            asyncio.create_task(_handle_server_uni_stream(stream, label, received, quiet_push))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"❌ [{label}] server uni stream error: {e}")


async def _handle_server_uni_stream(stream, label: str, received: list, quiet_push: bool):
    """Read a server-initiated uni stream. Peek at first byte to detect type:
       - 0x03 prefix = chat voice stream (always log)
       - anything else = test push stream (silence when quiet_push)"""
    try:
        first_chunk = await stream.read()
        if not first_chunk:
            return

        is_voice = (first_chunk[0] == STREAM_TYPE_VOICE)

        if is_voice:
            # Chat voice broadcast — always log
            logger.info(f"🚀 [{label}] Server voice stream opened (ID: {stream.stream_id})")
            data = first_chunk[1:]  # strip the 0x03 prefix
            if data:
                logger.info(f"🔊 [{label} VOICE-RX] ← {len(data)} bytes")
                received.append(("voice", data))
            # Continue reading voice data
            while True:
                chunk = await stream.read()
                if not chunk:
                    logger.info(f"🛑 [{label} VOICE-RX] Stream ended (ID: {stream.stream_id})")
                    break
                logger.info(f"🔊 [{label} VOICE-RX] ← {len(chunk)} bytes")
                received.append(("voice", chunk))
        else:
            # Test push stream — silently drain when quiet_push, otherwise log
            if not quiet_push:
                logger.info(f"🚀 [{label}] Server UNI stream (ID: {stream.stream_id})")
                text = first_chunk.decode("utf-8", errors="replace")
                logger.info(f"📥 [{label} SRV-UNI] ← {text!r}")
            # Drain remaining data
            while True:
                chunk = await stream.read()
                if not chunk:
                    break
                if not quiet_push:
                    text = chunk.decode("utf-8", errors="replace")
                    logger.info(f"📥 [{label} SRV-UNI] ← {text!r}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        if is_voice or not quiet_push:
            logger.error(f"❌ [{label} UNI] error: {e}")


async def listen_server_bidi_streams(session, label: str, received: list, quiet_push: bool = False):
    """Listen for server-initiated bidirectional streams."""
    try:
        async for stream in session.incoming_bidirectional_streams():
            if not quiet_push:
                logger.info(f"🚀 [{label}] Server opened BIDI stream (ID: {stream.stream_id})")
            asyncio.create_task(read_stream_forever(stream, f"{label} SRV-BIDI", received, quiet=quiet_push))
    except asyncio.CancelledError:
        pass


# ═══════════════════════════════════════════════════════════════════════
# 4. CHAT USER CLASS — wraps a single WebTransport session
# ═══════════════════════════════════════════════════════════════════════

class ChatUser:
    """Represents a connected chat user with dedicated streams."""

    def __init__(self, session, username: str, quiet_push: bool = False):
        self.session = session
        self.username = username
        self.quiet_push = quiet_push
        self.control_stream = None
        self.chat_stream = None
        self.voice_stream = None  # client → server uni stream

        # Collectors for received data
        self.control_received = []
        self.chat_received = []
        self.datagram_received = []
        self.voice_received = []

        # Background task handles
        self._bg_tasks = []

    async def setup(self):
        """Open all required streams and start listeners."""
        # ── 1. Open Control Stream (Bidirectional, prefix 0x01) ──
        self.control_stream = await self.session.create_bidirectional_stream()
        # Send the demux prefix byte
        await self.control_stream.write_all(data=bytes([STREAM_TYPE_CONTROL]), end_stream=False)
        logger.info(f"🔧 [{self.username}] Control stream opened (ID: {self.control_stream.stream_id})")

        # ── 2. Open Text Chat Stream (Bidirectional, prefix 0x02) ──
        self.chat_stream = await self.session.create_bidirectional_stream()
        await self.chat_stream.write_all(data=bytes([STREAM_TYPE_CHAT]), end_stream=False)
        logger.info(f"🔧 [{self.username}] Chat stream opened (ID: {self.chat_stream.stream_id})")

        # ── 3. Open Voice Stream (Unidirectional, prefix 0x03) ──
        self.voice_stream = await self.session.create_unidirectional_stream()
        await self.voice_stream.write_all(data=bytes([STREAM_TYPE_VOICE]), end_stream=False)
        logger.info(f"🔧 [{self.username}] Voice UNI stream opened (ID: {self.voice_stream.stream_id})")

        # ── 4. Start background listeners ──
        self._bg_tasks.append(asyncio.create_task(
            read_stream_forever(self.control_stream, f"{self.username} CTRL", self.control_received)
        ))
        self._bg_tasks.append(asyncio.create_task(
            read_stream_forever(self.chat_stream, f"{self.username} CHAT", self.chat_received)
        ))
        self._bg_tasks.append(asyncio.create_task(
            listen_datagrams(self.session, self.username, self.datagram_received)
        ))
        self._bg_tasks.append(asyncio.create_task(
            listen_server_uni_streams(self.session, self.username, self.voice_received, quiet_push=self.quiet_push)
        ))
        self._bg_tasks.append(asyncio.create_task(
            listen_server_bidi_streams(self.session, self.username, [], quiet_push=self.quiet_push)
        ))

        # Small delay to let streams register on server side
        await asyncio.sleep(0.3)

    async def join(self, room: str):
        """Send JOIN command on the control stream."""
        cmd = f"JOIN {room} {self.username}"
        await self.control_stream.write_all(data=cmd.encode("utf-8"), end_stream=False)
        logger.info(f"➡️  [{self.username}] Sent: {cmd}")
        await asyncio.sleep(0.5)

    async def leave(self):
        """Send LEAVE command on the control stream."""
        await self.control_stream.write_all(data=b"LEAVE", end_stream=False)
        logger.info(f"➡️  [{self.username}] Sent: LEAVE")
        await asyncio.sleep(0.5)

    async def send_text(self, message: str):
        """Send a text message on the chat stream."""
        await self.chat_stream.write_all(data=message.encode("utf-8"), end_stream=False)
        logger.info(f"💬 [{self.username}] Sent text: {message}")
        await asyncio.sleep(0.3)

    async def send_voice(self, data: bytes):
        """Send voice chunk on the voice uni stream."""
        await self.voice_stream.write_all(data=data, end_stream=False)
        logger.info(f"🎤 [{self.username}] Sent voice: {len(data)} bytes")
        await asyncio.sleep(0.3)

    async def send_typing(self, is_typing: bool):
        """Send typing indicator via datagram."""
        msg = f"TYPING {str(is_typing).lower()}"
        await self.session.send_datagram(data=msg.encode("utf-8"))
        logger.info(f"⌨️  [{self.username}] Sent datagram: {msg}")
        await asyncio.sleep(0.3)

    async def cleanup(self):
        """Cancel all background tasks."""
        for task in self._bg_tasks:
            task.cancel()
        await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        await self.session.close()


# ═══════════════════════════════════════════════════════════════════════
# 5. TEST SCENARIOS
# ═══════════════════════════════════════════════════════════════════════

async def test_single_user():
    """Test 1: Single user — join, chat, voice, typing, leave."""
    logger.info("=" * 70)
    logger.info("📋 TEST 1: Single User Flow")
    logger.info("=" * 70)

    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")

    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/chat")
        logger.info("✅ Connected to /chat")

        user = ChatUser(session, "Alice")
        await user.setup()

        # ── Join room ──
        await user.join("lobby")
        logger.info(f"   Control replies so far: {user.control_received}")

        # ── Send text messages ──
        await user.send_text("Hello, World!")
        await user.send_text("Testing from Python 🐍")
        await asyncio.sleep(0.5)
        logger.info(f"   Chat received so far: {user.chat_received}")

        # ── Send typing indicator via datagram ──
        await user.send_typing(True)
        await asyncio.sleep(0.3)
        await user.send_typing(False)
        await asyncio.sleep(0.5)
        logger.info(f"   Datagram received so far: {user.datagram_received}")

        # ── Send voice data ──
        fake_voice = b"\x00\x01\x02\x03" * 100  # 400 bytes of fake audio
        await user.send_voice(fake_voice)
        await asyncio.sleep(0.5)
        logger.info(f"   Voice received so far: {len(user.voice_received)} items")

        # ── Leave room ──
        await user.leave()
        logger.info(f"   Control replies after LEAVE: {user.control_received}")

        # ── Try chatting after leaving (should get error on control) ──
        await user.send_text("This should fail")
        await asyncio.sleep(0.5)
        logger.info(f"   Control replies (error expected): {user.control_received}")

        await user.cleanup()

    logger.info("✅ TEST 1 COMPLETE\n")


async def test_multi_user():
    """Test 2: Two users in the same room — message broadcast + typing indicators."""
    logger.info("=" * 70)
    logger.info("📋 TEST 2: Multi-User Broadcast")
    logger.info("=" * 70)

    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")

    async with WebTransportClient(config=config) as client1, \
               WebTransportClient(config=config) as client2:

        session1 = await client1.connect(url="https://localhost:4433/chat")
        session2 = await client2.connect(url="https://localhost:4433/chat")
        logger.info("✅ Both users connected")

        alice = ChatUser(session1, "Alice")
        bob   = ChatUser(session2, "Bob")

        await alice.setup()
        await bob.setup()

        # ── Both join the same room ──
        await alice.join("dev-room")
        await bob.join("dev-room")
        await asyncio.sleep(0.5)

        # Alice should have received Bob's JOIN system notification on her chat stream
        logger.info(f"   Alice chat (expect Bob join): {alice.chat_received}")
        # Bob should have Alice's join in control reply (but system broadcast goes to chat)
        logger.info(f"   Bob control: {bob.control_received}")

        # ── Alice sends a message → Bob should receive it ──
        await alice.send_text("Hey Bob, how's it going?")
        await asyncio.sleep(0.5)
        logger.info(f"   Bob chat (expect Alice's msg): {bob.chat_received}")

        # ── Bob replies → Alice should receive it ──
        await bob.send_text("Great Alice, thanks for asking!")
        await asyncio.sleep(0.5)
        logger.info(f"   Alice chat (expect Bob's msg): {alice.chat_received}")

        # ── Alice sends typing indicator → Bob should receive via datagram ──
        await alice.send_typing(True)
        await asyncio.sleep(0.5)
        logger.info(f"   Bob datagram (expect TYPING:Alice:true): {bob.datagram_received}")

        await alice.send_typing(False)
        await asyncio.sleep(0.5)
        logger.info(f"   Bob datagram (expect TYPING:Alice:false): {bob.datagram_received}")

        # ── Bob sends typing ──
        await bob.send_typing(True)
        await asyncio.sleep(0.5)
        logger.info(f"   Alice datagram (expect TYPING:Bob:true): {alice.datagram_received}")

        # ── Voice: Alice sends voice → Bob should receive on server uni stream ──
        voice_data = bytes(range(256)) * 2  # 512 bytes of test audio
        await alice.send_voice(voice_data)
        await asyncio.sleep(1.0)
        logger.info(f"   Bob voice received items: {len(bob.voice_received)}")

        # ── Alice leaves → Bob should get SYSTEM message ──
        await alice.leave()
        await asyncio.sleep(0.5)
        logger.info(f"   Bob chat (expect Alice left): {bob.chat_received}")

        # ── Bob sends message after Alice left → only Bob sees it ──
        await bob.send_text("Is anyone still here?")
        await asyncio.sleep(0.5)
        logger.info(f"   Bob chat (self-broadcast): {bob.chat_received}")

        await alice.cleanup()
        await bob.cleanup()

    logger.info("✅ TEST 2 COMPLETE\n")


async def test_room_isolation():
    """Test 3: Two users in different rooms — messages should NOT cross rooms."""
    logger.info("=" * 70)
    logger.info("📋 TEST 3: Room Isolation")
    logger.info("=" * 70)

    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")

    async with WebTransportClient(config=config) as client1, \
               WebTransportClient(config=config) as client2:

        session1 = await client1.connect(url="https://localhost:4433/chat")
        session2 = await client2.connect(url="https://localhost:4433/chat")

        alice = ChatUser(session1, "Alice")
        bob   = ChatUser(session2, "Bob")

        await alice.setup()
        await bob.setup()

        # Put them in DIFFERENT rooms
        await alice.join("room-alpha")
        await bob.join("room-beta")
        await asyncio.sleep(0.5)

        # Alice sends a message — Bob should NOT receive it
        await alice.send_text("Hello Alpha room!")
        await asyncio.sleep(0.5)
        logger.info(f"   Bob chat (should be EMPTY): {bob.chat_received}")

        # Bob sends a message — Alice should NOT receive it
        await bob.send_text("Hello Beta room!")
        await asyncio.sleep(0.5)
        logger.info(f"   Alice chat (should be EMPTY): {alice.chat_received}")

        # Alice sends typing — Bob should NOT get it
        await alice.send_typing(True)
        await asyncio.sleep(0.5)
        logger.info(f"   Bob datagram (should be EMPTY): {bob.datagram_received}")

        await alice.cleanup()
        await bob.cleanup()

    logger.info("✅ TEST 3 COMPLETE\n")


async def test_room_switching():
    """Test 4: User switches rooms — old room notified, new room notified."""
    logger.info("=" * 70)
    logger.info("📋 TEST 4: Room Switching")
    logger.info("=" * 70)

    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")

    async with WebTransportClient(config=config) as client1, \
               WebTransportClient(config=config) as client2:

        session1 = await client1.connect(url="https://localhost:4433/chat")
        session2 = await client2.connect(url="https://localhost:4433/chat")

        alice = ChatUser(session1, "Alice")
        bob   = ChatUser(session2, "Bob")

        await alice.setup()
        await bob.setup()

        # Both start in room-alpha
        await alice.join("room-alpha")
        await bob.join("room-alpha")
        await asyncio.sleep(0.5)

        # Verify they can talk
        await alice.send_text("Both in alpha!")
        await asyncio.sleep(0.5)
        logger.info(f"   Bob got message in alpha: {bob.chat_received}")

        # Alice switches to room-beta (re-JOIN)
        bob.chat_received.clear()
        await alice.join("room-beta")
        await asyncio.sleep(0.5)
        logger.info(f"   Alice control (new room OK): {alice.control_received}")
        logger.info(f"   Bob chat (Alice left system msg): {bob.chat_received}")

        # Now Alice sends message — Bob should NOT receive
        bob.chat_received.clear()
        await alice.send_text("Hello Beta from Alice")
        await asyncio.sleep(0.5)
        logger.info(f"   Bob chat (should be EMPTY now): {bob.chat_received}")

        await alice.cleanup()
        await bob.cleanup()

    logger.info("✅ TEST 4 COMPLETE\n")


async def test_edge_cases():
    """Test 5: Edge cases — chat before joining, leave without room, rapid fire."""
    logger.info("=" * 70)
    logger.info("📋 TEST 5: Edge Cases")
    logger.info("=" * 70)

    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")

    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/chat")

        user = ChatUser(session, "Charlie")
        await user.setup()

        # ── Send chat before joining any room → expect error on control stream ──
        await user.send_text("I haven't joined yet!")
        await asyncio.sleep(0.5)
        logger.info(f"   Control (expect error): {user.control_received}")

        # ── LEAVE without being in a room → expect error ──
        await user.leave()
        await asyncio.sleep(0.5)
        logger.info(f"   Control (expect error): {user.control_received}")

        # ── Invalid JOIN format ──
        await user.control_stream.write_all(data=b"JOIN singlearg", end_stream=False)
        await asyncio.sleep(0.5)
        logger.info(f"   Control (expect invalid JOIN error): {user.control_received}")

        # ── Valid JOIN, then rapid-fire messages ──
        await user.join("stress-room")
        for i in range(5):
            await user.send_text(f"Rapid message #{i}")
        await asyncio.sleep(1.0)
        logger.info(f"   Chat (expect rapid messages): {user.chat_received}")

        await user.cleanup()

    logger.info("✅ TEST 5 COMPLETE\n")


# ═══════════════════════════════════════════════════════════════════════
# 6. INTERACTIVE MODE
# ═══════════════════════════════════════════════════════════════════════

async def interactive_mode():
    """Run an interactive chat client — type messages in the terminal."""
    logger.info("=" * 70)
    logger.info("📋 INTERACTIVE MODE — Type messages to send")
    logger.info("   Commands: /join <room> <name> | /leave | /typing | /voice | /quit")
    logger.info("=" * 70)

    config = ClientConfig(verify_mode=ssl.CERT_NONE, log_level="WARNING")

    async with WebTransportClient(config=config) as client:
        session = await client.connect(url="https://localhost:4433/chat")
        logger.info("✅ Connected to /chat")

        user = ChatUser(session, "InteractiveUser", quiet_push=True)
        await user.setup()

        loop = asyncio.get_event_loop()

        try:
            while True:
                # Read from stdin without blocking the event loop
                line = await loop.run_in_executor(None, input, ">>> ")
                line = line.strip()
                if not line:
                    continue

                if line.startswith("/join "):
                    parts = line[6:].split(" ", 1)
                    if len(parts) == 2:
                        user.username = parts[1]
                        await user.join(parts[0])
                    else:
                        print("Usage: /join <room> <username>")
                elif line == "/leave":
                    await user.leave()
                elif line == "/typing":
                    await user.send_typing(True)
                    await asyncio.sleep(1.0)
                    await user.send_typing(False)
                elif line == "/voice":
                    await user.send_voice(b"\xDE\xAD\xBE\xEF" * 50)
                elif line == "/quit":
                    break
                else:
                    await user.send_text(line)

        except (KeyboardInterrupt, EOFError):
            pass

        await user.cleanup()

    logger.info("👋 Disconnected")


# ═══════════════════════════════════════════════════════════════════════
# 7. MAIN
# ═══════════════════════════════════════════════════════════════════════

async def run_all_tests():
    """Run all automated test scenarios sequentially."""
    logger.info("\n🚀 WebTransport Chat Handler — Full Test Suite\n")

    try:
        await test_single_user()
    except Exception as e:
        logger.error(f"❌ TEST 1 FAILED: {e}", exc_info=True)

    try:
        await test_multi_user()
    except Exception as e:
        logger.error(f"❌ TEST 2 FAILED: {e}", exc_info=True)

    try:
        await test_room_isolation()
    except Exception as e:
        logger.error(f"❌ TEST 3 FAILED: {e}", exc_info=True)

    try:
        await test_room_switching()
    except Exception as e:
        logger.error(f"❌ TEST 4 FAILED: {e}", exc_info=True)

    try:
        await test_edge_cases()
    except Exception as e:
        logger.error(f"❌ TEST 5 FAILED: {e}", exc_info=True)

    logger.info("\n✅ ALL TESTS FINISHED\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        try:
            asyncio.run(interactive_mode())
        except KeyboardInterrupt:
            print("\nBye!")
    else:
        try:
            asyncio.run(run_all_tests())
        except KeyboardInterrupt:
            print("\nTest terminated by user.")

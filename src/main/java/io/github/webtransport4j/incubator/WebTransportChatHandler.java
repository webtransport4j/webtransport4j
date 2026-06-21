package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.Future;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

/**
 * A real-life production-grade Chat Handler demonstrating WebTransport features.
 * 
 * Protocol Design:
 * <ul>
 *   <li><b>Control Stream (Bidi, Type 0x01)</b>: Handles authentication, join/leave commands.</li>
 *   <li><b>Text Chat Stream (Bidi, Type 0x02)</b>: Handles text messaging within a room.</li>
 *   <li><b>Voice Stream (Uni, Type 0x03)</b>: Client stream streams voice chunks (uni), server broadcasts via server uni-streams.</li>
 *   <li><b>Typing Status (Datagrams)</b>: Typing notifications (lossy, extremely low latency).</li>
 * </ul>
 */
public class WebTransportChatHandler implements WebTransportHandler {

  private static final Logger logger = Logger.getLogger(WebTransportChatHandler.class.getName());

  // In-memory registry of active users: Session -> ChatUser
  private final Map<WebTransportSession, ChatUser> users = new ConcurrentHashMap<>();

  // Demultiplexing headers
  private static final byte STREAM_TYPE_CONTROL = 0x01;
  private static final byte STREAM_TYPE_CHAT = 0x02;
  private static final byte STREAM_TYPE_VOICE = 0x03;

  @Override
  public void onSessionReady(WebTransportSession session) {
    long sessionId = session.getSessionStreamId();
    logger.info("💬 [CHAT] User session established. Session ID: " + sessionId);
    
    // Register the user with a default username until they send a JOIN command
    users.put(session, new ChatUser(session));
  }

  @Override
  public void onSessionClosed(WebTransportSession session) {
    ChatUser user = users.remove(session);
    if (user != null && user.username != null) {
      logger.info("💬 [CHAT] User '" + user.username + "' left the chat.");
      broadcastToRoom(user.room, "SYSTEM: " + user.username + " has left the room.", null);
      user.cleanup();
    }
  }

  @Override
  public void onIncomingStream(WebTransportSession session, WebTransportStream stream) {
    ChatUser user = users.get(session);
    if (user == null) {
      logger.warn("⚠️ Received stream for unregistered session: " + session.getSessionStreamId());
      stream.close();
      return;
    }

    // Set stream listeners
    stream.onClose(() -> logger.debug("💬 [CHAT] Stream " + stream.streamId() + " closed."));
    stream.onError(err -> logger.error("💬 [CHAT] Stream error on " + stream.streamId() + ": ", err));

    // Register raw data consumer to demultiplex stream purpose using the first byte
    stream.onData(new java.util.function.Consumer<ByteBuf>() {
      private byte streamPurpose = 0x00;
      private boolean purposeIdentified = false;

      @Override
      public void accept(ByteBuf data) {
        if (!purposeIdentified) {
          if (!data.isReadable()) return;
          streamPurpose = data.readByte();
          purposeIdentified = true;
          
          // Map stream in our user state for targeting later
          if (streamPurpose == STREAM_TYPE_CONTROL) {
            user.controlStream = stream;
            logger.info("💬 [CHAT] Control stream linked for User: " + user.username);
          } else if (streamPurpose == STREAM_TYPE_CHAT) {
            user.chatStream = stream;
            logger.info("💬 [CHAT] Text Chat stream linked for User: " + user.username);
          } else if (streamPurpose == STREAM_TYPE_VOICE) {
            logger.info("💬 [CHAT] Incoming Client Voice stream opened by User: " + user.username);
          }
        }

        // Process stream payload according to its tagged purpose
        if (data.isReadable()) {
          ByteBuf payload = data.readSlice(data.readableBytes());
          if (streamPurpose == STREAM_TYPE_CONTROL) {
            handleControlMessage(user, payload);
          } else if (streamPurpose == STREAM_TYPE_CHAT) {
            handleTextMessage(user, payload);
          } else if (streamPurpose == STREAM_TYPE_VOICE) {
            handleVoiceChunk(user, payload);
          }
        }
      }
    });
  }

  @Override
  public void onDatagramReceived(WebTransportSession session, ByteBuf data) {
    ChatUser user = users.get(session);
    if (user == null || user.room == null) return;

    String content = data.toString(StandardCharsets.UTF_8);
    // Typing Indicator Protocol: "TYPING <isTyping>"
    if (content.startsWith("TYPING ")) {
      boolean isTyping = Boolean.parseBoolean(content.substring(7).trim());
      String broadcastMsg = "TYPING:" + user.username + ":" + isTyping;
      
      // Broadcast typing indicator to all other users in the room via low-latency datagrams
      byte[] payloadBytes = broadcastMsg.getBytes(StandardCharsets.UTF_8);
      for (ChatUser roomMember : users.values()) {
        if (user.room.equals(roomMember.room) && roomMember.session != user.session) {
          roomMember.session.sendDatagram(payloadBytes);
        }
      }
    }
  }

  // --- Core Protocol Message Handlers ---

  private void handleControlMessage(ChatUser user, ByteBuf payload) {
    String command = payload.toString(StandardCharsets.UTF_8).trim();
    logger.info("💬 [CHAT-CONTROL] Command from " + user.username + ": " + command);

    // JOIN <room> <username>
    if (command.startsWith("JOIN ")) {
      String[] parts = command.substring(5).split(" ", 2);
      if (parts.length < 2) {
        sendControlReply(user, "ERROR: Invalid JOIN format. Use: JOIN <room> <username>");
        return;
      }
      String oldRoom = user.room;
      user.room = parts[0].trim();
      user.username = parts[1].trim();

      // Clean up previous room state if any
      if (oldRoom != null) {
        broadcastToRoom(oldRoom, "SYSTEM: " + user.username + " left the room.", user);
      }

      // Initialize outgoing server unidirectional voice broadcast stream for the user
      user.session.createUniStream().addListener((Future<WebTransportStream> f) -> {
        if (f.isSuccess()) {
          WebTransportStream serverUni = f.getNow();
          // Write demux prefix identifying it as a voice stream (0x03)
          ByteBuf prefix = Unpooled.buffer(1);
          prefix.writeByte(STREAM_TYPE_VOICE);
          serverUni.write(prefix);
          user.serverVoiceStream = serverUni;
          logger.info("💬 [CHAT] Outbound Server Voice stream initialized for " + user.username);
        }
      });

      sendControlReply(user, "OK: Joined room " + user.room + " as " + user.username);
      broadcastToRoom(user.room, "SYSTEM: " + user.username + " joined the room.", user);
    } 
    // LEAVE
    else if ("LEAVE".equals(command)) {
      if (user.room != null) {
        broadcastToRoom(user.room, "SYSTEM: " + user.username + " left the room.", user);
        sendControlReply(user, "OK: Left room " + user.room);
        user.room = null;
        if (user.serverVoiceStream != null) {
          user.serverVoiceStream.close();
          user.serverVoiceStream = null;
        }
      } else {
        sendControlReply(user, "ERROR: You are not in a room.");
      }
    }
  }

  private void handleTextMessage(ChatUser user, ByteBuf payload) {
    if (user.room == null) {
      sendControlReply(user, "ERROR: Join a room before chatting.");
      return;
    }
    String message = payload.toString(StandardCharsets.UTF_8);
    logger.info("💬 [CHAT-MSG] [" + user.room + "] " + user.username + ": " + message);

    String formattedMsg = user.username + ": " + message;
    broadcastToRoom(user.room, formattedMsg, null);
  }

  private void handleVoiceChunk(ChatUser user, ByteBuf payload) {
    if (user.room == null) return;

    logger.debug("💬 [CHAT-VOICE] Broadcast voice chunk from " + user.username + " (" + payload.readableBytes() + " bytes)");

    // Broadcast the voice bytes to everyone else in the same room using server-initiated voice streams
    for (ChatUser roomMember : users.values()) {
      if (user.room.equals(roomMember.room) && roomMember.session != user.session) {
        if (roomMember.serverVoiceStream != null && roomMember.serverVoiceStream.channel().isActive()) {
          // Send raw voice data to peer (retaining duplicate to safely share bytes across channels)
          roomMember.serverVoiceStream.write(payload.retainedDuplicate());
        }
      }
    }
  }

  // --- Helper Methods ---

  private void sendControlReply(ChatUser user, String reply) {
    if (user.controlStream != null && user.controlStream.channel().isActive()) {
      user.controlStream.writeText(reply);
    } else {
      logger.warn("⚠️ Control stream not active for user " + user.username + ", unable to send: " + reply);
    }
  }

  private void broadcastToRoom(String room, String message, ChatUser excludeUser) {
    if (room == null) return;
    
    for (ChatUser roomMember : users.values()) {
      if (room.equals(roomMember.room)) {
        if (excludeUser != null && roomMember.session == excludeUser.session) {
          continue; // Skip the sender
        }
        if (roomMember.chatStream != null && roomMember.chatStream.channel().isActive()) {
          roomMember.chatStream.writeText(message);
        }
      }
    }
  }

  // --- Inner Helper class representing Chat User State ---

  private static class ChatUser {
    final WebTransportSession session;
    String username;
    String room;

    // High-level wrapper stream references
    WebTransportStream controlStream;
    WebTransportStream chatStream;
    WebTransportStream serverVoiceStream;

    ChatUser(WebTransportSession session) {
      this.session = session;
      this.username = "Guest-" + session.getSessionStreamId();
    }

    void cleanup() {
      if (controlStream != null) controlStream.close();
      if (chatStream != null) chatStream.close();
      if (serverVoiceStream != null) serverVoiceStream.close();
    }
  }
}

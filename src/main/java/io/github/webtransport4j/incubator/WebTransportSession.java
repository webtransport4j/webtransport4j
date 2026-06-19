package io.github.webtransport4j.incubator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author https://github.com/sanjomo
 * @date 24/12/25 1:21 am
 */
public class WebTransportSession {

    private final long sessionStreamId;

    private final QuicStreamChannel connectStream;

    private final Set<QuicStreamChannel> activeClientInitiatedUni;
    private final Set<QuicStreamChannel> activeServerInitiatedUni;
    private final Set<QuicStreamChannel> activeClientInitiatedBi;
    private final Set<QuicStreamChannel> activeServerInitiatedBi;

    // Local stream limits (how many streams we allow the client to initiate)
    private final AtomicLong settingsMaxStreamsUni;
    private final AtomicLong settingsMaxStreamsBidi;
    private final AtomicLong settingsMaxData;

    // Peer stream limits (how many streams the client allows the server to
    // initiate)
    private final AtomicLong peerSettingsMaxStreamsUni;
    private final AtomicLong peerSettingsMaxStreamsBidi;
    private final AtomicLong peerSettingsMaxData;

    // Cumulative stream counters for streams initiated by the Client
    private final AtomicLong clientInitiatedStreamsUni = new AtomicLong(0L);
    private final AtomicLong clientInitiatedStreamsBidi = new AtomicLong(0L);

    // Cumulative stream counters for streams initiated by the Server
    private final AtomicLong serverInitiatedStreamsUni = new AtomicLong(0L);
    private final AtomicLong serverInitiatedStreamsBidi = new AtomicLong(0L);

    // Cumulative byte counters for session-level flow control
    private final AtomicLong cumulativeBytesSent = new AtomicLong(0L);
    private final AtomicLong cumulativeBytesReceived = new AtomicLong(0L);

    // Queue for pending writes when flow control blocks them
    private final Queue<PendingWrite> pendingWrites = new ConcurrentLinkedQueue<>();
    // Tracks the last peer limit for which a WT_DATA_BLOCKED capsule was sent
    private final AtomicLong lastSentDataBlockedLimit = new AtomicLong(-1L);

    private final boolean flowControlEnabled;

    // Initial allowed concurrent limits set at the start of the session
    private final long initialMaxStreamsUni;
    private final long initialMaxStreamsBidi;
    private final long initialMaxData;

    WebTransportSession(long sessionStreamId,
            QuicStreamChannel connectStream,
            long maxStreamsUni,
            long maxStreamsBidi,
            long maxData,
            long peerMaxStreamsUni,
            long peerMaxStreamsBidi,
            long peerMaxData,
            boolean flowControlEnabled) {
        this.sessionStreamId = sessionStreamId;
        this.connectStream = connectStream;
        this.settingsMaxStreamsUni = new AtomicLong(maxStreamsUni);
        this.settingsMaxStreamsBidi = new AtomicLong(maxStreamsBidi);
        this.initialMaxStreamsUni = maxStreamsUni;
        this.initialMaxStreamsBidi = maxStreamsBidi;
        this.settingsMaxData = new AtomicLong(maxData);
        this.initialMaxData = maxData;
        this.peerSettingsMaxStreamsUni = new AtomicLong(peerMaxStreamsUni);
        this.peerSettingsMaxStreamsBidi = new AtomicLong(peerMaxStreamsBidi);
        this.peerSettingsMaxData = new AtomicLong(peerMaxData);
        this.flowControlEnabled = flowControlEnabled;
        this.activeClientInitiatedBi = ConcurrentHashMap.newKeySet();
        this.activeServerInitiatedBi = ConcurrentHashMap.newKeySet();
        this.activeClientInitiatedUni = ConcurrentHashMap.newKeySet();
        this.activeServerInitiatedUni = ConcurrentHashMap.newKeySet();
    }

    public Set<QuicStreamChannel> getActiveClientInitiatedUni() {
        return activeClientInitiatedUni;
    }

    public Set<QuicStreamChannel> getActiveServerInitiatedUni() {
        return activeServerInitiatedUni;
    }

    public Set<QuicStreamChannel> getActiveClientInitiatedBi() {
        return activeClientInitiatedBi;
    }

    public Set<QuicStreamChannel> getActiveServerInitiatedBi() {
        return activeServerInitiatedBi;
    }

    public boolean isFlowControlEnabled() {
        return flowControlEnabled;
    }

    public long getSessionStreamId() {
        return sessionStreamId;
    }

    public QuicStreamChannel getConnectStream() {
        return connectStream;
    }

    public long getSettingsMaxStreamsUni() {
        return settingsMaxStreamsUni.get();
    }

    public long getSettingsMaxStreamsBidi() {
        return settingsMaxStreamsBidi.get();
    }

    public long getSettingsMaxData() {
        return settingsMaxData.get();
    }

    public long getPeerSettingsMaxStreamsUni() {
        return peerSettingsMaxStreamsUni.get();
    }

    public void setPeerSettingsMaxStreamsUni(long value) {
        this.peerSettingsMaxStreamsUni.set(value);
    }

    public long getPeerSettingsMaxStreamsBidi() {
        return peerSettingsMaxStreamsBidi.get();
    }

    public void setPeerSettingsMaxStreamsBidi(long value) {
        this.peerSettingsMaxStreamsBidi.set(value);
    }

    public long getPeerSettingsMaxData() {
        return peerSettingsMaxData.get();
    }

    public void setPeerSettingsMaxData(long value) {
        this.peerSettingsMaxData.set(value);
    }

    public long getClientInitiatedStreamsUni() {
        return clientInitiatedStreamsUni.get();
    }

    public long getClientInitiatedStreamsBidi() {
        return clientInitiatedStreamsBidi.get();
    }

    public long incrementAndGetClientInitiatedStreamsBidi() {
        return clientInitiatedStreamsBidi.incrementAndGet();
    }

    public long incrementAndGetClientInitiatedStreamsUni() {
        return clientInitiatedStreamsUni.incrementAndGet();
    }

    public void setClientInitiatedStreamsUni(long clientInitiatedStreamsUni) {
        this.clientInitiatedStreamsUni.set(clientInitiatedStreamsUni);
    }

    public void setClientInitiatedStreamsBidi(long clientInitiatedStreamsBidi) {
        this.clientInitiatedStreamsBidi.set(clientInitiatedStreamsBidi);
    }

    public long getInitialMaxStreamsUni() {
        return initialMaxStreamsUni;
    }

    public long getInitialMaxStreamsBidi() {
        return initialMaxStreamsBidi;
    }

    public void setSettingsMaxStreamsUni(long value) {
        this.settingsMaxStreamsUni.set(value);
    }

    public void setSettingsMaxStreamsBidi(long value) {
        this.settingsMaxStreamsBidi.set(value);
    }

    public long getServerInitiatedStreamsUni() {
        return serverInitiatedStreamsUni.get();
    }

    public long getServerInitiatedStreamsBidi() {
        return serverInitiatedStreamsBidi.get();
    }

    public long incrementAndGetServerInitiatedStreamsUni() {
        return serverInitiatedStreamsUni.incrementAndGet();
    }

    public long incrementAndGetServerInitiatedStreamsBidi() {
        return serverInitiatedStreamsBidi.incrementAndGet();
    }

    public long getInitialMaxData() {
        return initialMaxData;
    }

    public void setSettingsMaxData(long value) {
        this.settingsMaxData.set(value);
    }

    public long getCumulativeBytesSent() {
        return cumulativeBytesSent.get();
    }

    public long getCumulativeBytesReceived() {
        return cumulativeBytesReceived.get();
    }

    public long incrementCumulativeBytesSent(long value) {
        return this.cumulativeBytesSent.addAndGet(value);
    }

    public long incrementCumulativeBytesReceived(long value) {
        return this.cumulativeBytesReceived.addAndGet(value);
    }

    // --- Flow control pending write support ---

    /**
     * Returns the queue of pending writes that are blocked by flow control.
     */
    public Queue<PendingWrite> getPendingWrites() {
        return pendingWrites;
    }

    /**
     * Returns the AtomicLong tracking the last peer limit for which
     * a WT_DATA_BLOCKED capsule was sent. Callers use CAS operations on this.
     */
    public AtomicLong getLastSentDataBlockedLimit() {
        return lastSentDataBlockedLimit;
    }

    /**
     * Flushes pending writes that now fit within the updated peer limit.
     * Called when a WT_MAX_DATA capsule is received and the limit increases.
     */
    public void flushPendingWrites() {
        long peerLimit = getPeerSettingsMaxData();
        long currentSent = getCumulativeBytesSent();
        while (true) {
            PendingWrite pw = pendingWrites.peek();
            if (pw == null) {
                break;
            }
            int bytesToWrite = pw.getData().readableBytes();
            if (currentSent + bytesToWrite > peerLimit) {
                break; // Still blocked
            }
            // Remove and flush — bytes will be counted when the write
            // re-enters the pipeline through RawWebTransportHandler.write()
            pw = pendingWrites.poll();
            if (pw != null) {
                currentSent += pw.getData().readableBytes();
                pw.getStreamChannel().writeAndFlush(pw.getData(), pw.getPromise());
            }
        }
    }

    /**
     * Releases all pending writes and fails their promises. Called on session close.
     */
    public void cleanupPendingWrites(Throwable cause) {
        PendingWrite pw;
        while ((pw = pendingWrites.poll()) != null) {
            try { pw.getData().release(); } catch (Exception ignored) {}
            try { pw.getPromise().tryFailure(cause); } catch (Exception ignored) {}
        }
    }

    /**
     * Represents a write that was buffered because it would exceed
     * the session-level flow control limit.
     */
    public static class PendingWrite {
        private final QuicStreamChannel streamChannel;
        private final ByteBuf data;
        private final ChannelPromise promise;

        public PendingWrite(QuicStreamChannel streamChannel, ByteBuf data, ChannelPromise promise) {
            this.streamChannel = streamChannel;
            this.data = data;
            this.promise = promise;
        }

        public QuicStreamChannel getStreamChannel() {
            return streamChannel;
        }

        public ByteBuf getData() {
            return data;
        }

        public ChannelPromise getPromise() {
            return promise;
        }
    }
}

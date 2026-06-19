package io.github.webtransport4j.incubator;

import io.netty.handler.codec.quic.QuicStreamChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    // Peer stream limits (how many streams the client allows the server to initiate)
    private final AtomicLong peerSettingsMaxStreamsUni;
    private final AtomicLong peerSettingsMaxStreamsBidi;
    private final AtomicLong peerSettingsMaxData;

    // Cumulative stream counters for streams initiated by the Client
    private final AtomicLong clientInitiatedStreamsUni = new AtomicLong(0L);
    private final AtomicLong clientInitiatedStreamsBidi = new AtomicLong(0L);

    // Cumulative stream counters for streams initiated by the Server
    private final AtomicLong serverInitiatedStreamsUni = new AtomicLong(0L);
    private final AtomicLong serverInitiatedStreamsBidi = new AtomicLong(0L);

    private final boolean flowControlEnabled;

    // Initial allowed concurrent limits set at the start of the session
    private final long initialMaxStreamsUni;
    private final long initialMaxStreamsBidi;

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
}

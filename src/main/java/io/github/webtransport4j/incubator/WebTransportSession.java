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
    private final AtomicLong settingsMaxStreamsUni;
    private final AtomicLong settingsMaxStreamsBidi;
    private final AtomicLong settingsMaxData;
    private final AtomicLong peerSettingsMaxStreamsUni;
    private final AtomicLong peerSettingsMaxStreamsBidi;
    private final AtomicLong peerSettingsMaxData;
    private final AtomicLong currentStreamsUni = new AtomicLong(0L);
    private final AtomicLong currentStreamsBidi = new AtomicLong(0L);
    private final boolean flowControlEnabled;

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

    public long getCurrentStreamsUni() {
        return currentStreamsUni.get();
    }

    public long getCurrentStreamsBidi() {
        return currentStreamsBidi.get();
    }
    public long incrementAndGetCurrentStreamsBidi() {
        return currentStreamsBidi.incrementAndGet();
    }
    public long incrementAndGetCurrentStreamsUni() {
        return currentStreamsUni.incrementAndGet();
    }   

    public void setCurrentStreamsUni(long currentStreamsUni) {
        this.currentStreamsUni.set(currentStreamsUni);
    }

    public void setCurrentStreamsBidi(long currentStreamsBidi) {
        this.currentStreamsBidi.set(currentStreamsBidi);
    }   
}

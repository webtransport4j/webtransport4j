package io.github.webtransport4j.incubator;

import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.internal.ConcurrentSet;

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
    private final Set<QuicStreamChannel> activeUni;
    private final Set<QuicStreamChannel> activeBi;
    private final AtomicLong settingsInitialMaxStreamsUni;
    private final AtomicLong settingsInitialMaxStreamsBidi;
    private final AtomicLong settingsInitialMaxData;
    private final AtomicLong currentStreamsUni = new AtomicLong(0L);
    private final AtomicLong currentStreamsBidi = new AtomicLong(0L);
    private final boolean flowControlEnabled;

    WebTransportSession(long sessionStreamId,
                        QuicStreamChannel connectStream,
                        long initialMaxStreamsUni,
                        long initialMaxStreamsBidi,
                        long initialMaxData,
                    boolean flowControlEnabled) {
        this.sessionStreamId = sessionStreamId;
        this.connectStream = connectStream;
        this.settingsInitialMaxStreamsUni = new AtomicLong(initialMaxStreamsUni);
        this.settingsInitialMaxStreamsBidi = new AtomicLong(initialMaxStreamsBidi);
        this.settingsInitialMaxData = new AtomicLong(initialMaxData);
        this.flowControlEnabled = flowControlEnabled;
        this.activeBi = ConcurrentHashMap.newKeySet();
        this.activeUni = ConcurrentHashMap.newKeySet();
    }

    public Set<QuicStreamChannel> getActiveUni() {
        return activeUni;
    }

    public Set<QuicStreamChannel> getActiveBi() {
        return activeBi;
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

    public long getSettingsInitialMaxStreamsUni() {
        return settingsInitialMaxStreamsUni.get();
    }

    public long getSettingsInitialMaxStreamsBidi() {
        return settingsInitialMaxStreamsBidi.get();
    }

    public long getSettingsInitialMaxData() {
        return settingsInitialMaxData.get();
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

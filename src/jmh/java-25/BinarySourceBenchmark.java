package io.github.webtransport4j.jmh;

import io.github.webtransport4j.api.BinarySource;
import io.github.webtransport4j.api.BinarySources;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class BinarySourceBenchmark {

    private byte[] data;
    private ByteBuffer byteBuffer;
    private MemorySegment memorySegment;
    private Arena arena;

    private ByteBuffer dstBuffer;

    @Setup(Level.Trial)
    public void setup() {
        data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        byteBuffer = ByteBuffer.wrap(data);
        arena = Arena.ofShared();
        memorySegment = arena.allocate(data.length);
        MemorySegment.copy(data, 0, memorySegment, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, data.length);

        dstBuffer = ByteBuffer.allocateDirect(128);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    public int testByteArraySource() throws IOException {
        dstBuffer.clear();
        try (BinarySource source = BinarySources.fromByteArray(data)) {
            return source.read(dstBuffer);
        }
    }

    @Benchmark
    public int testByteBufferSource() throws IOException {
        dstBuffer.clear();
        byteBuffer.clear();
        try (BinarySource source = BinarySources.fromByteBuffer(byteBuffer)) {
            return source.read(dstBuffer);
        }
    }

    @Benchmark
    public int testMemorySegmentSource() throws IOException {
        dstBuffer.clear();
        try (BinarySource source = BinarySources.fromMemorySegment(memorySegment)) {
            return source.read(dstBuffer);
        }
    }
}

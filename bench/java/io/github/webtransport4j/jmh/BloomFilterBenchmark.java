package io.github.webtransport4j.jmh;

import io.github.webtransport4j.server.IpBloomFilter;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class BloomFilterBenchmark {

    @Param({"100", "1000", "10000"})
    public int capacity;

    @Param({"0.01", "0.000001"})
    public double fpp;

    private IpBloomFilter filter;
    private String hitIp;
    private String missIp;

    @Setup(Level.Trial)
    public void setup() {
        filter = new IpBloomFilter(capacity, fpp);

        for (int i = 0; i < capacity; i++) {
            int part2 = (i / 65536) % 256;
            int part3 = (i / 256) % 256;
            int part4 = i % 256;
            filter.add("192." + part2 + "." + part3 + "." + part4);
        }

        hitIp = "192.0.0.1";
        missIp = "10.0.0.1";
    }

    @Benchmark
    public boolean testMightContainHit() {
        return filter.mightContain(hitIp);
    }

    @Benchmark
    public boolean testMightContainMiss() {
        return filter.mightContain(missIp);
    }
}

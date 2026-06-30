package io.github.webtransport4j.jmh;

import io.github.webtransport4j.server.IpFilterEngine;
import io.github.webtransport4j.server.IpPrefixTrieEngine;
import io.github.webtransport4j.server.NettyLinearIpFilterEngine;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** JMH benchmark for IpRateLimit performance. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class IpRateLimitBenchmark {

  @Param({"10", "100", "1000"})
  public int ruleCount;

  private IpFilterEngine<Integer> nettyEngine;
  private IpFilterEngine<Integer> trieEngine;

  private String matchIp;
  private String missIp;

  /** Setup. */
  @Setup(Level.Trial)
  public void setup() {
    nettyEngine = new NettyLinearIpFilterEngine<>();
    trieEngine = new IpPrefixTrieEngine<>();

    // Add rules
    for (int i = 0; i < ruleCount; i++) {
      int part2 = (i / 65536) % 256;
      int part3 = (i / 256) % 256;
      int part4 = i % 256;
      String ip = "192." + part2 + "." + part3 + "." + part4;
      nettyEngine.addRule(ip, i);
      trieEngine.addRule(ip, i);
    }

    // Add specific rule to verify match
    String targetIp = "10.0.0.1";
    nettyEngine.addRule(targetIp, 9999);
    trieEngine.addRule(targetIp, 9999);

    matchIp = targetIp;
    missIp = "8.8.8.8";
  }

  /** Test Netty Match. */
  @Benchmark
  public Integer testNettyMatch() {
    return nettyEngine.match(matchIp);
  }

  /** Test Trie Match. */
  @Benchmark
  public Integer testTrieMatch() {
    return trieEngine.match(matchIp);
  }

  /** Test Netty Miss. */
  @Benchmark
  public Integer testNettyMiss() {
    return nettyEngine.match(missIp);
  }

  /** Test Trie Miss. */
  @Benchmark
  public Integer testTrieMiss() {
    return trieEngine.match(missIp);
  }
}

package io.github.webtransport4j.jmh;

import io.github.webtransport4j.server.HmacQuicTokenHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** JMH benchmark for Token performance. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(0)
public class TokenBenchmark {

  private HmacQuicTokenHandler tokenHandler;
  private ByteBuf dcid;
  private ByteBuf outToken;
  private InetSocketAddress clientAddress;
  private ByteBuf validToken;

  /** Sets up test fixtures. */
  @Setup
  public void setup() {
    tokenHandler = new HmacQuicTokenHandler();
    dcid = Unpooled.directBuffer(20);
    // Fill connection ID bytes
    for (int i = 0; i < 20; i++) {
      dcid.writeByte(i);
    }

    outToken = Unpooled.directBuffer(256);
    clientAddress = new InetSocketAddress("192.168.1.1", 12345);

    validToken = Unpooled.directBuffer(256);
    tokenHandler.writeToken(validToken, dcid, clientAddress);
  }

  /** Cleans up test fixtures. */
  @TearDown
  public void tearDown() {
    dcid.release();
    outToken.release();
    validToken.release();
  }

  /** Test Generate Token. */
  @Benchmark
  public boolean testGenerateToken() {
    outToken.clear();
    dcid.readerIndex(0);
    return tokenHandler.writeToken(outToken, dcid, clientAddress);
  }

  @Benchmark
  public int testValidateToken() {
    validToken.readerIndex(0);
    return tokenHandler.validateToken(validToken, clientAddress);
  }
}

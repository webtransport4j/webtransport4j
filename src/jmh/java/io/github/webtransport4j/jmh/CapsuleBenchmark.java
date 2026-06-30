package io.github.webtransport4j.jmh;

import io.github.webtransport4j.server.WebTransportUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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

/** JMH benchmark for Capsule performance. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class CapsuleBenchmark {

  private ByteBuf writeBuf;
  private ByteBuf readBuf;

  /** Sets up test fixtures. */
  @Setup
  public void setup() {
    writeBuf = Unpooled.directBuffer(1024);
    readBuf = Unpooled.directBuffer(1024);
    // Write a standard WT_MAX_STREAMS capsule (type 0x190B4D3F, value 100)
    WebTransportUtils.writeCapsule(readBuf, 0x190B4D3FL, 100);
  }

  /** Cleans up test fixtures. */
  @TearDown
  public void tearDown() {
    writeBuf.release();
    readBuf.release();
  }

  /** Test Write Capsule. */
  @Benchmark
  public void testWriteCapsule() {
    writeBuf.clear();
    WebTransportUtils.writeCapsule(writeBuf, 0x190B4D3FL, 100);
  }

  /** Test Read Capsule Fields. */
  @Benchmark
  public long testReadCapsuleFields() {
    readBuf.readerIndex(0);
    long capType = WebTransportUtils.readVariableLengthInt(readBuf);
    long capLen = WebTransportUtils.readVariableLengthInt(readBuf);
    long capVal = WebTransportUtils.readVariableLengthInt(readBuf);
    return capType + capLen + capVal;
  }
}

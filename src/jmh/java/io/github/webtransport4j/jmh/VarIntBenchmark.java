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

/** JMH benchmark for VarInt performance. */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class VarIntBenchmark {

  private ByteBuf writeBuf;
  private ByteBuf readBuf1;
  private ByteBuf readBuf2;
  private ByteBuf readBuf4;
  private ByteBuf readBuf8;

  /** Sets up test fixtures. */
  @Setup
  public void setup() {
    writeBuf = Unpooled.directBuffer(1024);

    readBuf1 = Unpooled.directBuffer(8);
    WebTransportUtils.writeVarInt(readBuf1, 37); // 1 byte

    readBuf2 = Unpooled.directBuffer(8);
    WebTransportUtils.writeVarInt(readBuf2, 1000); // 2 bytes

    readBuf4 = Unpooled.directBuffer(8);
    WebTransportUtils.writeVarInt(readBuf4, 100000); // 4 bytes

    readBuf8 = Unpooled.directBuffer(8);
    WebTransportUtils.writeVarInt(readBuf8, 10000000000L); // 8 bytes
  }

  /** Cleans up test fixtures. */
  @TearDown
  public void tearDown() {
    writeBuf.release();
    readBuf1.release();
    readBuf2.release();
    readBuf4.release();
    readBuf8.release();
  }

  @Benchmark
  public void testWriteVarInt1Byte() {
    writeBuf.clear();
    WebTransportUtils.writeVarInt(writeBuf, 37);
  }

  @Benchmark
  public void testWriteVarInt2Bytes() {
    writeBuf.clear();
    WebTransportUtils.writeVarInt(writeBuf, 1000);
  }

  @Benchmark
  public void testWriteVarInt4Bytes() {
    writeBuf.clear();
    WebTransportUtils.writeVarInt(writeBuf, 100000);
  }

  @Benchmark
  public void testWriteVarInt8Bytes() {
    writeBuf.clear();
    WebTransportUtils.writeVarInt(writeBuf, 10000000000L);
  }

  @Benchmark
  public long testReadVarInt1Byte() {
    readBuf1.readerIndex(0);
    return WebTransportUtils.readVariableLengthInt(readBuf1);
  }

  @Benchmark
  public long testReadVarInt2Bytes() {
    readBuf2.readerIndex(0);
    return WebTransportUtils.readVariableLengthInt(readBuf2);
  }

  @Benchmark
  public long testReadVarInt4Bytes() {
    readBuf4.readerIndex(0);
    return WebTransportUtils.readVariableLengthInt(readBuf4);
  }

  @Benchmark
  public long testReadVarInt8Bytes() {
    readBuf8.readerIndex(0);
    return WebTransportUtils.readVariableLengthInt(readBuf8);
  }
}

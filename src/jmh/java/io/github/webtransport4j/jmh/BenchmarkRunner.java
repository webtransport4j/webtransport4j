package io.github.webtransport4j.jmh;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/** Main runner for JMH benchmarks. */
public class BenchmarkRunner {
  /** Main. */
  public static void main(String[] args) throws Exception {
    boolean quick = Boolean.getBoolean("benchmark.quick");
    int forks =
        quick
            ? 0
            : Integer.getInteger(
                "benchmark.forks",
                0); // Default to 0 (in-process) to avoid ForkedMain classloader issues
    int warmup = quick ? 1 : 5;
    int measurement = quick ? 1 : 5;

    Options opt =
        new OptionsBuilder()
            .include(".*Benchmark.*")
            .forks(forks)
            .warmupIterations(warmup)
            .measurementIterations(measurement)
            .build();

    new Runner(opt).run();
  }
}

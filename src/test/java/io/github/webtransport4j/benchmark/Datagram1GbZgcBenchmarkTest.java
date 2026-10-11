package io.github.webtransport4j.benchmark;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/** Regression checks for benchmark measurement reporting. */
public class Datagram1GbZgcBenchmarkTest {
  @Test
  public void formatsDefinedAndUndefinedMemoryPoolMaximums() {
    Assert.assertEquals("N/A", Datagram1GbZgcBenchmark.formatMemoryPoolMax(-1));
    Assert.assertEquals("0.00 MiB", Datagram1GbZgcBenchmark.formatMemoryPoolMax(0));
    Assert.assertEquals("1.00 MiB", Datagram1GbZgcBenchmark.formatMemoryPoolMax(1024 * 1024));
    Assert.assertEquals("1.50 MiB", Datagram1GbZgcBenchmark.formatMemoryPoolMax(1536 * 1024));
  }

  @Test
  public void labelsBinaryUnitsConsistently() throws Exception {
    Path report = Files.createTempFile("datagram-report-units", ".md");
    long mib = 1024 * 1024;
    try {
      Datagram1GbZgcBenchmark.writeMarkdownReport(
          report.toString(), 1024, mib, mib, true,
          1, 1, 1024, 0, 0, 0, mib, 2 * mib, 3 * mib, 4 * mib,
          Collections.emptyMap(), Collections.singletonMap("worker", 5 * mib), Collections.emptyList());
      String markdown = new String(Files.readAllBytes(report), StandardCharsets.UTF_8);
      Assert.assertTrue(markdown.contains("1,048,576 bytes (1.00 MiB / 0.00 GiB)"));
      Assert.assertTrue(markdown.contains("| **Data Throughput** | **1.00 MiB/s** |"));
      Assert.assertTrue(markdown.contains("| **Initial Used Heap** | 1,048,576 bytes (1.00 MiB) |"));
      Assert.assertTrue(markdown.contains("| **Final Used Heap** | 2,097,152 bytes (2.00 MiB) |"));
      Assert.assertTrue(markdown.contains("| **Peak Used Heap** | 3,145,728 bytes (3.00 MiB) |"));
      Assert.assertTrue(markdown.contains("| **Heap Delta** | 1,048,576 bytes (1.00 MiB) |"));
      Assert.assertTrue(markdown.contains("| **Off-Heap Direct Memory** | 4,194,304 bytes (4.00 MiB) |"));
      Assert.assertTrue(markdown.contains("| Thread Name | Bytes Allocated | MiB Allocated |"));
      Assert.assertTrue(markdown.contains("| `worker` | 5,242,880 bytes | 5.00 MiB |"));
      Assert.assertTrue(markdown.contains("| Memory Pool | Used (MiB) | Max (MiB) |"));
      Assert.assertFalse(markdown.contains(" MB"));
      Assert.assertFalse(markdown.contains("Megabytes"));
    } finally {
      Files.deleteIfExists(report);
    }
  }

  @Test
  public void separatesPauseAndCycleTime() {
    Map<String, long[]> collectors = new LinkedHashMap<>();
    collectors.put("ZGC Minor Pauses", new long[] {30, 0});
    collectors.put("ZGC Major Pauses", new long[] {45, 0});
    collectors.put("ZGC Minor Cycles", new long[] {10, 74});
    collectors.put("ZGC Major Cycles", new long[] {9, 228});
    Assert.assertEquals(0, Datagram1GbZgcBenchmark.collectionTime(collectors, "Pauses"));
    Assert.assertEquals(302, Datagram1GbZgcBenchmark.collectionTime(collectors, "Cycles"));
    collectors.put("ZGC Major Pauses", new long[] {45, 12});
    Assert.assertEquals(12, Datagram1GbZgcBenchmark.collectionTime(collectors, "Pauses"));
  }

  @Test
  public void reportsCompletionIndependentlyFromLatchAndMeasuredPauses() throws Exception {
    Path report = Files.createTempFile("datagram-report", ".md");
    try {
      for (boolean completedOnTime : new boolean[] {false, true}) {
        for (long totalBytes : new long[] {0, 512, 1024, 2048}) {
          for (long pauses : new long[] {0, 12}) {
            Datagram1GbZgcBenchmark.writeMarkdownReport(
                report.toString(), 1, totalBytes, 1024, completedOnTime,
                1, 1, 1, 94, pauses, 302, 0, 0, 0, 0,
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList());
            String markdown = new String(Files.readAllBytes(report), StandardCharsets.UTF_8);
            Assert.assertTrue(markdown.contains("| **Run Status** | **"
                + (totalBytes >= 1024 ? "COMPLETE" : "INCOMPLETE") + "** |"));
            Assert.assertTrue(markdown.contains("| **Completed On Time (latch)** | "
                + completedOnTime + " |"));
            Assert.assertTrue(markdown.contains("| **Target Reached** | "
                + (totalBytes >= 1024) + " |"));
            Assert.assertTrue(markdown.contains("| **Target Payload** | 1,024 bytes |"));
            Assert.assertTrue(markdown.contains("| **Total GC Pause Time** | **"
                + pauses + " ms** |"));
            Assert.assertTrue(markdown.contains("| **Total ZGC Cycle Time** | **302 ms** |"));
            Assert.assertFalse(markdown.contains("sub-millisecond pause"));
            Assert.assertTrue(markdown.contains(Datagram1GbZgcBenchmark.objectHeaderMode()));
          }
        }
      }
    } finally {
      Files.deleteIfExists(report);
    }
  }
}

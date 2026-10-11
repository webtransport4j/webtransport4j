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

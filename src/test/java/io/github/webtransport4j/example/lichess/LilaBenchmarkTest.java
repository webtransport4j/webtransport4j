package io.github.webtransport4j.example.lichess;

import io.github.webtransport4j.example.lichess.benchmark.LilaBenchmarkRunner;
import io.github.webtransport4j.example.lichess.benchmark.LilaBenchmarkSuite;
import io.github.webtransport4j.example.lichess.benchmark.LilaBenchmarkSuite.ScenarioStats;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Automated JUnit integration test running the Lichess comparison benchmark suite.
 */
public class LilaBenchmarkTest {

  private static LilaBenchmarkSuite suite;

  /**
   * Sets up benchmark servers.
   *
   * @throws Exception if server startup fails
   */
  @BeforeClass
  public static void setUp() throws Exception {
    suite = new LilaBenchmarkSuite(100, 20); // Fast iteration count for automated CI
    suite.startServers();
  }

  /**
   * Tears down benchmark servers.
   */
  @AfterClass
  public static void tearDown() {
    if (suite != null) {
      suite.stopServers();
    }
  }

  /**
   * Runs the comparative benchmark suite and verifies metrics.
   *
   * @throws Exception if benchmark run fails
   */
  @Test
  public void testLilaComparativeBenchmark() throws Exception {
    List<ScenarioStats> results = suite.runAllBenchmarks();
    Assert.assertNotNull(results);
    Assert.assertFalse(results.isEmpty());

    // Print summary to test log
    LilaBenchmarkRunner.printTable(results);

    // Verify each scenario collected valid statistical metrics
    for (ScenarioStats s : results) {
      Assert.assertTrue("Count must be positive for " + s.scenario, s.count > 0);
      Assert.assertTrue("p50 latency must be >= 0 for " + s.scenario, s.medianMs >= 0);
      Assert.assertTrue("Max latency must be >= min for " + s.scenario, s.maxMs >= s.minMs);
      Assert.assertTrue("Jitter std dev must be >= 0 for " + s.scenario, s.jitterStdDevMs >= 0);
    }
  }
}

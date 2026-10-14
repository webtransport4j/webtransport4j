package io.github.webtransport4j.test;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

/**
 * JUnit {@link RunListener} that generates test TLS certificates before test execution.
 */
public class TestCertificateRunListener extends RunListener {

  @Override
  public void testRunStarted(Description description) {
    TestCertificates.ensureTestCertificatesExist();
  }
}

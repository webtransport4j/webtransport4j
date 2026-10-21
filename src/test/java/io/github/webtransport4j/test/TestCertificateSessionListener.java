package io.github.webtransport4j.test;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

/**
 * JUnit Platform {@link LauncherSessionListener} that generates test TLS certificates
 * before any test execution begins.
 *
 * <p>Registered via {@code META-INF/services/org.junit.platform.launcher.LauncherSessionListener}.
 */
public class TestCertificateSessionListener implements LauncherSessionListener {

  @Override
  public void launcherSessionOpened(LauncherSession session) {
    TestCertificates.ensureTestCertificatesExist();
  }
}

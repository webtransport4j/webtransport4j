package io.github.webtransport4j.server;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.webtransport4j.api.WebTransportHandler;
import io.github.webtransport4j.test.TestCertificates;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests profile resolution and profile-based TLS certificate enforcement.
 */
public class ProfileConfigurationTest {

  private String originalProfile;
  private String originalDevMode;
  private String originalKeyPath;
  private String originalCertPath;

  /** Sets up test properties before each test. */
  @BeforeEach
  public void setUp() {
    originalProfile = System.getProperty("webtransport4j.profile");
    originalDevMode = System.getProperty("webtransport4j.dev_mode");
    originalKeyPath = System.getProperty("webtransport4j.ssl.key.path");
    originalCertPath = System.getProperty("webtransport4j.ssl.cert.path");
    WebTransportConfig.clearProfileCache();
  }

  /** Restores system properties after each test. */
  @AfterEach
  public void tearDown() {
    restoreProperty("webtransport4j.profile", originalProfile);
    restoreProperty("webtransport4j.dev_mode", originalDevMode);
    restoreProperty("webtransport4j.ssl.key.path", originalKeyPath);
    restoreProperty("webtransport4j.ssl.cert.path", originalCertPath);
    WebTransportConfig.clearProfileCache();
  }

  private static void restoreProperty(String key, String original) {
    if (original == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, original);
    }
  }

  @Test
  public void prodProfileRejectsMissingCertsEvenIfDevModeSet() {
    System.setProperty("webtransport4j.profile", "prod");
    System.setProperty("webtransport4j.dev_mode", "true");
    System.clearProperty("webtransport4j.ssl.key.path");
    System.clearProperty("webtransport4j.ssl.cert.path");
    WebTransportConfig.clearProfileCache();

    assertEquals("prod", WebTransportConfig.getActiveProfile());
    assertFalse(WebTransportConfig.isDevOrTestProfile());

    try (WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .defaultHandler(new WebTransportHandler() {})
            .build()) {
      IllegalStateException ex = assertThrows(IllegalStateException.class, server::start);
      assertTrue(ex.getMessage().contains("SSL private key path and certificate path"));
      assertFalse(server.isStarted());
    }
  }

  @Test
  public void customProfileRejectsMissingCerts() {
    System.setProperty("webtransport4j.profile", "staging");
    System.setProperty("webtransport4j.dev_mode", "true");
    System.clearProperty("webtransport4j.ssl.key.path");
    System.clearProperty("webtransport4j.ssl.cert.path");
    WebTransportConfig.clearProfileCache();

    assertEquals("staging", WebTransportConfig.getActiveProfile());
    assertFalse(WebTransportConfig.isDevOrTestProfile());

    try (WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .defaultHandler(new WebTransportHandler() {})
            .build()) {
      IllegalStateException ex = assertThrows(IllegalStateException.class, server::start);
      assertTrue(ex.getMessage().contains("SSL private key path and certificate path"));
      assertFalse(server.isStarted());
    }
  }

  @Test
  public void serverWithExplicitCertsStartsSuccessfully() throws Exception {
    try (TestCertificates.TestCertificateBundle bundle = TestCertificates.generate()) {
      try (WebTransportServer server =
          WebTransportServer.builder()
              .port(0)
              .transportType("nio")
              .ssl(bundle.getKeyPath(), bundle.getCertPath())
              .defaultHandler(new WebTransportHandler() {})
              .build()) {
        server.start();
        assertTrue(server.isStarted());
        assertTrue(server.getPort() > 0);
      }
    }
  }

  @Test
  public void testProfileReadsTestCertsFromTestConfig() throws Exception {
    System.setProperty("webtransport4j.profile", "test");
    WebTransportConfig.clearProfileCache();

    assertEquals("test", WebTransportConfig.getActiveProfile());
    assertTrue(WebTransportConfig.isTestProfile());
    assertTrue(WebTransportConfig.isDevOrTestProfile());

    try (WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .defaultHandler(new WebTransportHandler() {})
            .build()) {
      server.start();
      assertTrue(server.isStarted());
      assertTrue(server.getPort() > 0);
    }
  }

  @Test
  public void commaSeparatedProfilesPreferProd() {
    System.clearProperty("webtransport4j.dev_mode");
    for (String profiles :
        new String[] {
          "prod,dev", "dev,prod", "prod,test", "test,prod", "custom,dev,prod", " DEV , PrOd , test "
        }) {
      System.setProperty("webtransport4j.profile", profiles);
      assertEquals("prod", WebTransportConfig.getActiveProfile(), profiles);
      assertFalse(WebTransportConfig.isDevOrTestProfile(), profiles);
      assertFalse(WebTransportConfig.getBoolean("webtransport4j.dev_mode", false), profiles);
    }
  }

  @Test
  public void commaSeparatedProfilesNormalizeToDevOrTest() {
    System.setProperty("webtransport4j.profile", "custom,dev");
    assertEquals("dev", WebTransportConfig.getActiveProfile());

    System.setProperty("webtransport4j.profile", "custom,test");
    assertEquals("test", WebTransportConfig.getActiveProfile());
  }
}

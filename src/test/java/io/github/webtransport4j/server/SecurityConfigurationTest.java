package io.github.webtransport4j.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.github.webtransport4j.security.ClientAuthMode;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.security.cert.X509Certificate;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.junit.Test;

/** Security configuration must fail explicitly rather than silently weaken TLS. */
public class SecurityConfigurationTest {
  @Test(timeout = 10000)
  public void suppliedContextRejectsClientAuthAndTrustOptions() throws Exception {
    QuicSslContext context = mock(QuicSslContext.class);
    for (ClientAuthMode mode : ClientAuthMode.values()) {
      try (WebTransportServer server =
          WebTransportServer.builder()
              .port(0)
              .transportType("nio")
              .sslContext(context)
              .clientAuth(mode)
              .build()) {
        IllegalArgumentException failure =
            assertThrows(IllegalArgumentException.class, server::start);
        assertTrue(failure.getMessage().contains("supplied sslContext"));
        assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
      }
    }
    try (WebTransportServer server =
        WebTransportServer.builder()
            .port(0)
            .transportType("nio")
            .sslContext(context)
            .trustManager(new File("unused-ca.pem"))
            .build()) {
      assertThrows(IllegalArgumentException.class, server::start);
    }
  }

  @Test(timeout = 10000)
  public void suppliedContextRejectsConfiguredAuthAndTrust() throws Exception {
    withProperty(
        "webtransport4j.ssl.client_auth",
        "REQUIRE",
        () -> {
          try (WebTransportServer server =
              WebTransportServer.builder()
                  .port(0)
                  .transportType("nio")
                  .sslContext(mock(QuicSslContext.class))
                  .build()) {
            assertThrows(IllegalArgumentException.class, server::start);
          }
        });
    withProperty(
        "webtransport4j.ssl.trust_cert.path",
        "missing-ca.pem",
        () -> {
          try (WebTransportServer server =
              WebTransportServer.builder()
                  .port(0)
                  .transportType("nio")
                  .sslContext(mock(QuicSslContext.class))
                  .build()) {
            assertThrows(IllegalArgumentException.class, server::start);
          }
        });
  }

  @Test(timeout = 10000)
  public void invalidAuthModeFailsStartup() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    try {
      withProperty(
          "webtransport4j.ssl.client_auth",
          "REQUIER",
          () -> {
            try (WebTransportServer server = server(certificate).build()) {
              IllegalArgumentException failure =
                  assertThrows(IllegalArgumentException.class, server::start);
              assertTrue(failure.getMessage().contains("ssl.client_auth"));
              assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
            }
          });
    } finally {
      certificate.delete();
    }
  }

  @Test(timeout = 10000)
  public void missingOrBlankConfiguredTrustFileFailsStartup() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    try {
      for (String path :
          new String[] {
            certificate.privateKey() + ".missing", "  ", certificate.certificate().getParent()
          }) {
        withProperty(
            "webtransport4j.ssl.trust_cert.path",
            path,
            () -> {
              try (WebTransportServer server =
                  server(certificate).clientAuth(ClientAuthMode.REQUIRE).build()) {
                IllegalArgumentException failure =
                    assertThrows(IllegalArgumentException.class, server::start);
                assertTrue(failure.getMessage().contains("readable file"));
                assertEquals(WebTransportServer.ServerState.STOPPED, server.getState());
              }
            });
      }
    } finally {
      certificate.delete();
    }
  }

  @Test(timeout = 15000)
  public void eachTrustOverloadReplacesEveryOtherSource() {
    File file = new File("old-ca.pem");
    X509Certificate[] certificates = {mock(X509Certificate.class)};
    TrustManagerFactory factory = mock(TrustManagerFactory.class);
    TrustManager manager = mock(TrustManager.class);
    for (int oldSource = 0; oldSource < 4; oldSource++) {
      for (int newSource = 0; newSource < 4; newSource++) {
        WebTransportServerBuilder builder = WebTransportServer.builder();
        setTrust(builder, oldSource, file, certificates, factory, manager);
        setTrust(builder, newSource, file, certificates, factory, manager);
        assertEquals(newSource == 0 ? file : null, builder.getTrustCertFile());
        assertArrayEquals(newSource == 1 ? certificates : null, builder.getTrustCertificates());
        assertSame(newSource == 2 ? factory : null, builder.getTrustManagerFactory());
        assertSame(newSource == 3 ? manager : null, builder.getTrustManager());
      }
    }
  }

  @Test(timeout = 10000)
  public void replacementFactoryIsAppliedInsteadOfStaleFile() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    TrustManagerFactory factory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init((java.security.KeyStore) null);
    try (WebTransportServer server =
        server(certificate)
            .trustManager(new File(certificate.privateKey() + ".missing"))
            .trustManager(factory)
            .build()) {
      server.start();
      assertTrue(server.isStarted());
    } finally {
      certificate.delete();
    }
  }

  @Test(timeout = 10000)
  public void certificatesAreSnapshottedAndNullResetsPreviousSource() {
    X509Certificate trusted = mock(X509Certificate.class);
    X509Certificate[] input = {trusted};
    WebTransportServerBuilder builder = WebTransportServer.builder().trustManager(input);
    input[0] = mock(X509Certificate.class);
    assertSame(trusted, builder.getTrustCertificates()[0]);
    builder.trustManager((TrustManager) null);
    assertNull(builder.getTrustCertificates());
  }

  @Test(timeout = 10000)
  public void explicitEmptyCertificatesOverrideConfiguredTrustPath() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    try {
      withProperty(
          "webtransport4j.ssl.trust_cert.path",
          certificate.privateKey() + ".missing",
          () -> {
            try (WebTransportServer server =
                server(certificate)
                    .clientAuth(ClientAuthMode.REQUIRE)
                    .trustManager(new X509Certificate[0])
                    .build()) {
              server.start();
              assertTrue(server.isStarted());
            }
          });
    } finally {
      certificate.delete();
    }
  }

  private static WebTransportServerBuilder server(SelfSignedCertificate certificate) {
    return WebTransportServer.builder()
        .port(0)
        .transportType("nio")
        .ssl(
            certificate.privateKey().getAbsolutePath(),
            certificate.certificate().getAbsolutePath());
  }

  private static void setTrust(
      WebTransportServerBuilder builder,
      int source,
      File file,
      X509Certificate[] certificates,
      TrustManagerFactory factory,
      TrustManager manager) {
    switch (source) {
      case 0:
        builder.trustManager(file);
        break;
      case 1:
        builder.trustManager(certificates);
        break;
      case 2:
        builder.trustManager(factory);
        break;
      default:
        builder.trustManager(manager);
        break;
    }
  }

  private static void withProperty(String key, String value, CheckedAction action)
      throws Exception {
    String previous = System.getProperty(key);
    System.setProperty(key, value);
    try {
      action.run();
    } finally {
      if (previous == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, previous);
      }
    }
  }

  private interface CheckedAction {
    void run() throws Exception;
  }
}

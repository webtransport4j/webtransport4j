package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.github.webtransport4j.security.ClientAuthMode;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

/** Integration tests for programmatic TLS certificate reloading. */
public class TlsCertificateReloaderProgrammaticTest {
  @Test(timeout = 30000)
  public void testProgrammaticCertificateReload() throws Exception {
    SelfSignedCertificate initial = new SelfSignedCertificate("localhost");
    SelfSignedCertificate rotated = new SelfSignedCertificate("localhost");
    try (WebTransportServer server = new WebTransportServerBuilder()
        .port(0)
        .ssl(initial.privateKey().getAbsolutePath(), initial.certificate().getAbsolutePath())
        .build()) {
      server.start();
      server.reloadTlsCertificate(rotated.certificate(), rotated.privateKey());
      try (QuicConcurrencyIntegrationTest.Client client =
          new QuicConcurrencyIntegrationTest.Client(server.getPort(), rotated)) {
        assertEquals(rotated.cert(),
            (X509Certificate) client.quic.sslEngine().getSession().getPeerCertificates()[0]);
        assertTrue(server.isRunning());
      }
    } finally {
      initial.delete();
      rotated.delete();
    }
  }

  @Test(timeout = 30000)
  public void pemReloadPreservesRequiredClientAuthenticationAndTrust() throws Exception {
    SelfSignedCertificate initial = new SelfSignedCertificate("localhost");
    SelfSignedCertificate rotated = new SelfSignedCertificate("localhost");
    SelfSignedCertificate trusted = new SelfSignedCertificate("client");
    SelfSignedCertificate untrusted = new SelfSignedCertificate("other-client");
    try (WebTransportServer server = new WebTransportServerBuilder()
        .port(0)
        .ssl(initial.privateKey().getAbsolutePath(), initial.certificate().getAbsolutePath())
        .clientAuth(ClientAuthMode.REQUIRE)
        .trustManager(trusted.certificate())
        .build()) {
      server.start();
      server.reloadTlsCertificate(rotated.certificate(), rotated.privateKey());
      try (QuicConcurrencyIntegrationTest.Client client =
          new QuicConcurrencyIntegrationTest.Client(server.getPort(), rotated, trusted)) {
        assertEquals(rotated.cert(),
            (X509Certificate) client.quic.sslEngine().getSession().getPeerCertificates()[0]);
      }
      assertRejected(server.getPort(), rotated, null);
      assertRejected(server.getPort(), rotated, untrusted);
    } finally {
      initial.delete();
      rotated.delete();
      trusted.delete();
      untrusted.delete();
    }
  }

  private static void assertRejected(int port, SelfSignedCertificate serverCertificate,
      SelfSignedCertificate identity) throws Exception {
    try (QuicConcurrencyIntegrationTest.Client ignored =
        new QuicConcurrencyIntegrationTest.Client(port, serverCertificate, identity)) {
      fail("Reloaded mTLS server accepted an unauthenticated or untrusted client");
    } catch (ExecutionException | TimeoutException expected) {
      // The handshake or CONNECT cannot complete without a trusted client identity.
    }
  }
}

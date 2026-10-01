package io.github.webtransport4j.server;

import static org.junit.Assert.assertTrue;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.Test;

/**
 * Integration test for programmatic TLS certificate reloading.
 */
public class TlsCertificateReloaderProgrammaticTest {

  @Test
  public void testProgrammaticCertificateReload() throws Exception {
    WebTransportServer server =
        new WebTransportServerBuilder()
            .port(0)
            .build();

    SelfSignedCertificate rotatedCert = null;
    try {
      server.start();
      assertTrue(server.isRunning());

      rotatedCert = new SelfSignedCertificate("rotated.example.com");
      server.reloadTlsCertificate(rotatedCert.certificate(), rotatedCert.privateKey());

      assertTrue(server.isRunning());
    } finally {
      if (rotatedCert != null) {
        rotatedCert.delete();
      }
      server.close();
    }
  }
}

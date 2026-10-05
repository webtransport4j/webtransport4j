package io.github.webtransport4j.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for TLS certificate hot reload watcher. */
public class TlsCertificateHotReloadTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test(timeout = 10000)
  public void failedReloadRetriesWithSecurityCustomization() throws Exception {
    SelfSignedCertificate certificate = new SelfSignedCertificate("localhost");
    AtomicInteger customized = new AtomicInteger();
    AtomicInteger installed = new AtomicInteger();
    TlsCertificateWatcher watcher =
        new TlsCertificateWatcher(
            certificate.privateKey().getAbsolutePath(),
            certificate.certificate().getAbsolutePath(),
            context -> {
              if (installed.getAndIncrement() == 0) {
                throw new IllegalStateException("Simulated context installation failure");
              }
            },
            builder -> {
              builder
                  .clientAuth(io.netty.handler.ssl.ClientAuth.REQUIRE)
                  .trustManager(certificate.certificate());
              customized.incrementAndGet();
            },
            1);
    try {
      assertFalse(watcher.checkAndReload());
      assertTrue(watcher.checkAndReload());
      assertFalse(watcher.checkAndReload());
      assertEquals(2, customized.get());
      assertEquals(2, installed.get());
    } finally {
      watcher.stop();
      certificate.delete();
    }
  }

  @Test
  public void testTlsCertificateHotReloadWatcher() throws Exception {
    SelfSignedCertificate cert1 = new SelfSignedCertificate("localhost");
    File keyFile = tempFolder.newFile("key.pem");
    File certFile = tempFolder.newFile("cert.pem");

    // Copy initial cert files
    Files.copy(cert1.privateKey().toPath(), keyFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(
        cert1.certificate().toPath(), certFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    AtomicReference<QuicSslContext> sslCtxRef = new AtomicReference<>();
    TlsCertificateWatcher watcher =
        new TlsCertificateWatcher(
            keyFile.getAbsolutePath(), certFile.getAbsolutePath(), sslCtxRef::set, 1);

    // Initial check (no modification timestamp change yet)
    boolean reloaded = watcher.checkAndReload();

    // Sleep to ensure filesystem modification timestamp updates
    Thread.sleep(1100);

    // Generate new certificate and overwrite file
    SelfSignedCertificate cert2 = new SelfSignedCertificate("localhost");
    Files.copy(
        cert2.certificate().toPath(), certFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    certFile.setLastModified(System.currentTimeMillis());

    // Trigger checkAndReload
    boolean reloadedAfterMod = watcher.checkAndReload();
    assertTrue(reloadedAfterMod);
    assertNotNull(sslCtxRef.get());

    watcher.stop();
  }
}

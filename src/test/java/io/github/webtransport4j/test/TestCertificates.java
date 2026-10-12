package io.github.webtransport4j.test;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import java.io.File;

/**
 * Test utility providing self-signed TLS certificates for tests.
 */
public final class TestCertificates {

  private TestCertificates() {}

  /**
   * Generates a temporary self-signed certificate bundle for testing.
   *
   * @return a new {@link TestCertificateBundle}
   * @throws Exception if generation fails
   */
  public static TestCertificateBundle generate() throws Exception {
    X509Bundle bundle =
        new CertificateBuilder()
            .subject("CN=localhost")
            .setIsCertificateAuthority(true)
            .addSanDnsName("localhost")
            .rsa2048()
            .buildSelfSigned();
    File keyFile = bundle.toTempPrivateKeyPem();
    File certFile = bundle.toTempCertChainPem();
    return new TestCertificateBundle(keyFile, certFile);
  }

  /**
   * Holds temporary certificate and private key files for test cleanup.
   */
  public static final class TestCertificateBundle implements AutoCloseable {
    private final File keyFile;
    private final File certFile;

    TestCertificateBundle(File keyFile, File certFile) {
      this.keyFile = keyFile;
      this.certFile = certFile;
    }

    public File getKeyFile() {
      return keyFile;
    }

    public File getCertFile() {
      return certFile;
    }

    public String getKeyPath() {
      return keyFile.getAbsolutePath();
    }

    public String getCertPath() {
      return certFile.getAbsolutePath();
    }

    @Override
    public void close() {
      if (keyFile != null && keyFile.exists()) {
        keyFile.delete();
      }
      if (certFile != null && certFile.exists()) {
        certFile.delete();
      }
    }
  }
}

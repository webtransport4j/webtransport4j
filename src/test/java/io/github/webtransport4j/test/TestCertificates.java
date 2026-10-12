package io.github.webtransport4j.test;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Test utility providing self-signed TLS certificates for tests.
 */
public final class TestCertificates {

  private TestCertificates() {}

  /**
   * Ensures that test certificates exist in {@code src/test/resources/test-key.pem} and
   * {@code src/test/resources/test-cert.pem}. If they do not exist, generates them automatically.
   */
  public static synchronized void ensureTestCertificatesExist() {
    File keyFile = new File("src/test/resources/test-key.pem");
    File certFile = new File("src/test/resources/test-cert.pem");
    if (keyFile.exists() && certFile.exists() && keyFile.length() > 0 && certFile.length() > 0) {
      return;
    }
    generateStaticFiles(keyFile, certFile);
  }

  private static void generateStaticFiles(File keyFile, File certFile) {
    try {
      File parent = keyFile.getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }
      X509Bundle bundle =
          new CertificateBuilder()
              .subject("CN=localhost")
              .setIsCertificateAuthority(true)
              .addSanDnsName("localhost")
              .rsa2048()
              .buildSelfSigned();

      Files.write(keyFile.toPath(), bundle.getPrivateKeyPEM().getBytes(StandardCharsets.UTF_8));
      Files.write(
          certFile.toPath(), bundle.getCertificatePathPEM().getBytes(StandardCharsets.UTF_8));

      File targetClasses = new File("target/test-classes");
      if (targetClasses.exists() && targetClasses.isDirectory()) {
        Files.write(
            new File(targetClasses, "test-key.pem").toPath(),
            bundle.getPrivateKeyPEM().getBytes(StandardCharsets.UTF_8));
        Files.write(
            new File(targetClasses, "test-cert.pem").toPath(),
            bundle.getCertificatePathPEM().getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to generate test certificates", e);
    }
  }

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

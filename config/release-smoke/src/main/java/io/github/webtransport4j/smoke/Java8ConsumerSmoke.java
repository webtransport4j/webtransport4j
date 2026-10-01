package io.github.webtransport4j.smoke;

import io.github.webtransport4j.api.BinarySource;
import io.github.webtransport4j.api.BinarySources;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Minimal downstream API and runtime smoke test compiled and executed on Java 8. */
public final class Java8ConsumerSmoke {

  private Java8ConsumerSmoke() {}

  public static void main(String[] args) throws Exception {
    byte[] expected = new byte[] {1, 2, 3, 4};
    ByteBuffer destination = ByteBuffer.allocate(expected.length);

    try (BinarySource source = BinarySources.fromByteArray(expected)) {
      if (source.size() != expected.length || source.read(destination) != expected.length) {
        throw new AssertionError("Unexpected BinarySource size or read length");
      }
    }

    if (!Arrays.equals(expected, destination.array())) {
      throw new AssertionError("BinarySource corrupted the payload");
    }

    System.out.println("Java 8 downstream compatibility smoke test passed");
  }
}

package io.github.webtransport4j.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.junit.Test;

/** Tests for {@link OriginValidator} and strict origin validation logic. */
public class OriginValidatorTest {

  @Test
  public void emptyAndBlankAllowlistsDenyAll() {
    for (boolean requireHeader : new boolean[] {false, true}) {
      for (java.util.List<String> patterns :
          java.util.Arrays.asList(
              Collections.<String>emptyList(), java.util.Arrays.asList(null, "", "  "))) {
        OriginValidator validator = OriginValidator.fromCollection(patterns, requireHeader);
        assertFalse(validator.validate("https://unlisted.example", "unlisted.example"));
        assertFalse(validator.validate(null, "unlisted.example"));
      }
    }
  }

  @Test
  public void explicitWildcardStillRequiresConfiguredOriginHeader() {
    OriginValidator validator =
        OriginValidator.fromCollection(Collections.singletonList("*"), true);
    assertFalse(validator.validate(null, "example.com"));
    assertFalse(validator.validate("", "example.com"));
    assertFalse(validator.validate("  ", "example.com"));
    assertTrue(validator.validate("https://example.com", null));
    assertTrue(OriginValidator.wildcard("*").validate(null, null));
  }

  /** Tests that allowAll permits any origin and authority. */
  @Test
  public void testAllowAll() {
    final OriginValidator validator = OriginValidator.allowAll();
    assertTrue(validator.validate("https://evil.com", "localhost:4433"));
    assertTrue(validator.validate(null, "localhost:4433"));
    assertTrue(validator.validate("https://good.com", null));
  }

  /** Tests that exact matching correctly validates scheme, host, and port. */
  @Test
  public void testExactMatching() {
    final OriginValidator validator =
        OriginValidator.exact("https://example.com", "api.internal.com");

    assertTrue(validator.validate("https://example.com", "example.com:4433"));
    assertTrue(validator.validate("https://api.internal.com", "api.internal.com:4433"));
    assertFalse(validator.validate("https://other.com", "example.com:4433"));
    assertFalse(validator.validate(null, "example.com:4433"));
  }

  /** Tests that wildcard domains match subdomains but not unrelated domains. */
  @Test
  public void testWildcardMatching() {
    final OriginValidator validator = OriginValidator.wildcard("*.example.com", "localhost");

    assertTrue(validator.validate("https://sub.example.com", "example.com"));
    assertTrue(validator.validate("https://deep.sub.example.com:8443", "example.com"));
    assertTrue(validator.validate("https://example.com", "example.com"));
    assertTrue(validator.validate("http://localhost:3000", "localhost"));
    assertFalse(validator.validate("https://fakeexample.com", "example.com"));
    assertFalse(validator.validate("https://attacker.org", "example.com"));
  }

  /**
   * Tests that fromCollection with requireOriginHeader=true rejects requests missing Origin header.
   */
  @Test
  public void testRequireOriginHeader() {
    final OriginValidator strictValidator =
        OriginValidator.fromCollection(Collections.singletonList("example.com"), true);

    assertTrue(strictValidator.validate("https://example.com", "example.com"));
    assertFalse(strictValidator.validate(null, "example.com"));
    assertFalse(strictValidator.validate("", "example.com"));

    final OriginValidator relaxedValidator =
        OriginValidator.fromCollection(Collections.singletonList("example.com"), false);
    assertTrue(relaxedValidator.validate(null, "example.com"));
    assertFalse(relaxedValidator.validate(null, "badhost.com"));
  }

  /** Tests that extractHost extracts the hostname from various URI formats. */
  @Test
  public void testExtractHost() {
    assertTrue(
        "example.com"
            .equalsIgnoreCase(OriginValidator.extractHost("https://example.com:8443/chat")));
    assertTrue("localhost".equalsIgnoreCase(OriginValidator.extractHost("http://localhost:3000")));
    assertTrue("foo.bar".equalsIgnoreCase(OriginValidator.extractHost("foo.bar:4433")));
  }
}

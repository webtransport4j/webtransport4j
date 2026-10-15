package io.github.webtransport4j.server;

import io.github.webtransport4j.internal.EventLoopSafety;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configuration utility for WebTransport server settings. */
public class WebTransportConfig {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportConfig.class);

  private static final String DEFAULT_PROFILE = "prod";
  private static final String DYNAMIC_CONFIG_FILENAME = "webtransport-dynamic.properties";
  private static final Properties staticProperties = new Properties();
  private static final Map<String, Properties> profilePropertiesCache = new ConcurrentHashMap<>();
  private static final AtomicLong revision = new AtomicLong();
  // Published generations are never mutated; writers replace them under the revision monitor.
  private static volatile Properties dynamicProperties = new Properties();
  private static volatile Properties programmaticProperties = new Properties();

  private WebTransportConfig() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated");
  }

  static {
    loadStaticConfig();
    Properties initialDynamic = new Properties();
    loadDynamicConfig(initialDynamic);
    dynamicProperties = initialDynamic;
  }

  private static void loadStaticConfig() {
    File localFile = new File("webtransport.properties");
    if (localFile.exists()) {
      try (InputStream in = new FileInputStream(localFile)) {
        staticProperties.load(in);
        logger.info(
            "📡 Loaded static configuration from local filesystem: {}",
            localFile.getAbsolutePath());
        return;
      } catch (Exception e) {
        logger.error("❌ Failed to load local properties file, falling back", e);
      }
    }
    try (InputStream in =
        WebTransportConfig.class.getClassLoader().getResourceAsStream("webtransport.properties")) {
      if (in != null) {
        staticProperties.load(in);
        logger.info("📡 Loaded default static configuration from classpath resources");
      } else {
        logger.warn("⚠️ webtransport.properties not found. Using fallback defaults.");
      }
    } catch (Exception e) {
      logger.error("❌ Failed to load static properties from classpath", e);
    }
  }

  private static void loadDynamicConfig(Properties target) {
    File localFile = new File(DYNAMIC_CONFIG_FILENAME);
    if (localFile.exists()) {
      try (InputStream in = new FileInputStream(localFile)) {
        target.load(in);
        logger.info(
            "📡 Loaded dynamic configuration from local filesystem: {}",
            localFile.getAbsolutePath());
      } catch (Exception e) {
        logger.error("❌ Failed to load local dynamic properties file, falling back", e);
      }
    }
  }

  /**
   * Resolves configuration key with precedence: 1. Java System Property (-Dkey=value) 2.
   * Environment Variable (ENV_KEY_NAME) 3. dynamic properties 4. static properties
   */
  public static @Nullable String get(@NonNull String key, @Nullable String defaultValue) {
    return getVal(key, defaultValue);
  }

  public static @NonNull String getNonNull(@NonNull String key, @NonNull String defaultValue) {
    return getVal(key, defaultValue);
  }

  /** Captures one dynamic configuration generation for related reads. */
  public static @NonNull Snapshot snapshot() {
    return new Snapshot(dynamicProperties, programmaticProperties);
  }

  /** A stable dynamic generation; system properties and environment retain their precedence. */
  public static final class Snapshot {
    private final Properties properties;
    private final Properties programmatic;

    private Snapshot(Properties properties, Properties programmatic) {
      this.properties = properties;
      this.programmatic = programmatic;
    }

    /** Resolves a key against the captured dynamic generation. */
    public @Nullable String get(@NonNull String key, @Nullable String defaultValue) {
      return getVal(properties, programmatic, key, defaultValue);
    }

    /** Resolves a non-null value against the captured dynamic generation. */
    public @NonNull String getNonNull(@NonNull String key, @NonNull String defaultValue) {
      return getVal(properties, programmatic, key, defaultValue);
    }

    /** Resolves an integer using the same parsing and fallback as the ordinary getter. */
    public int getInt(@NonNull String key, int defaultValue) {
      return WebTransportConfig.getInt(properties, programmatic, key, defaultValue);
    }

    /** Resolves a long using the same parsing and fallback as the ordinary getter. */
    public long getLong(@NonNull String key, long defaultValue) {
      return WebTransportConfig.getLong(properties, programmatic, key, defaultValue);
    }

    /** Resolves a boolean against the captured dynamic generation. */
    public boolean getBoolean(@NonNull String key, boolean defaultValue) {
      return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }
  }

  private static String getVal(String key, String defaultVal) {
    return getVal(dynamicProperties, programmaticProperties, key, defaultVal);
  }

  private static String getVal(
      Properties properties, Properties programmatic, String key, String defaultVal) {
    // 1. Check System Properties (-Dserver.port=...)
    String value = System.getProperty(key);
    if (value != null) {
      return value;
    }
    // 2. Check Environment Variables (SERVER_PORT=...)
    String envKey = key.toUpperCase(Locale.ROOT).replace('.', '_');
    value = System.getenv(envKey);
    if (value != null) {
      return value;
    }
    // 3. Check programmatic properties
    if (programmatic != null) {
      value = programmatic.getProperty(key);
      if (value != null) {
        return value;
      }
    }
    // 4. Check dynamic properties
    if (properties != null) {
      value = properties.getProperty(key);
      if (value != null) {
        return value;
      }
    }
    // 5. Check active profile properties (if not querying the profile property itself)
    if (!"webtransport4j.profile".equals(key)) {
      String profile = getActiveProfile(properties, programmatic);
      if (!DEFAULT_PROFILE.equals(profile)) {
        Properties profileProps = getProfileProperties(profile);
        if (profileProps != null) {
          value = profileProps.getProperty(key);
          if (value != null) {
            return value;
          }
        }
      }
    }
    // 6. Fallback to static properties file
    return staticProperties.getProperty(key, defaultVal);
  }

  /**
   * Returns the currently active configuration profile (e.g. {@code "prod"}, {@code "dev"},
   * {@code "test"}).
   *
   * <p>Precedence order:
   * <ol>
   *   <li>Java System Property ({@code -Dwebtransport4j.profile=...})</li>
   *   <li>Environment Variable ({@code WEBTRANSPORT4J_PROFILE=...})</li>
   *   <li>Framework active profiles (Spring {@code spring.profiles.active}, Quarkus {@code quarkus.profile})</li>
   *   <li>Programmatic properties ({@code setProperty("webtransport4j.profile", ...)})</li>
   *   <li>Dynamic properties ({@code webtransport4j.profile})</li>
   *   <li>Static properties in {@code webtransport.properties} ({@code webtransport4j.profile})</li>
   *   <li>Default: {@code "prod"}</li>
   * </ol>
   *
   * @return active profile name normalized to lowercase
   */
  public static @NonNull String getActiveProfile() {
    return getActiveProfile(dynamicProperties, programmaticProperties);
  }

  private static @NonNull String getActiveProfile(
      Properties dynamicProps, Properties progProps) {
    String profile = System.getProperty("webtransport4j.profile");
    if (profile != null && !profile.trim().isEmpty()) {
      return normalizeProfile(profile);
    }
    String envProfile = System.getenv("WEBTRANSPORT4J_PROFILE");
    if (envProfile != null && !envProfile.trim().isEmpty()) {
      return normalizeProfile(envProfile);
    }
    String springProfile = System.getProperty("spring.profiles.active");
    if (springProfile != null && !springProfile.trim().isEmpty()) {
      return normalizeProfile(springProfile);
    }
    String quarkusProfile = System.getProperty("quarkus.profile");
    if (quarkusProfile != null && !quarkusProfile.trim().isEmpty()) {
      return normalizeProfile(quarkusProfile);
    }
    if (progProps != null) {
      String configured = progProps.getProperty("webtransport4j.profile");
      if (configured != null && !configured.trim().isEmpty()) {
        return normalizeProfile(configured);
      }
    }
    if (dynamicProps != null) {
      String configured = dynamicProps.getProperty("webtransport4j.profile");
      if (configured != null && !configured.trim().isEmpty()) {
        return normalizeProfile(configured);
      }
    }
    String configured = staticProperties.getProperty("webtransport4j.profile");
    if (configured != null && !configured.trim().isEmpty()) {
      return normalizeProfile(configured);
    }
    return DEFAULT_PROFILE;
  }

  /** Returns {@code true} if the current active profile is {@code "dev"}. */
  public static boolean isDevProfile() {
    return "dev".equalsIgnoreCase(getActiveProfile());
  }

  /** Returns {@code true} if the current active profile is {@code "test"}. */
  public static boolean isTestProfile() {
    return "test".equalsIgnoreCase(getActiveProfile());
  }

  /** Returns {@code true} if the current active profile is {@code "dev"} or {@code "test"}. */
  public static boolean isDevOrTestProfile() {
    String profile = getActiveProfile();
    return "dev".equalsIgnoreCase(profile) || "test".equalsIgnoreCase(profile);
  }

  static void clearProfileCache() {
    profilePropertiesCache.clear();
  }

  private static String normalizeProfile(@Nullable String raw) {
    if (raw == null) {
      return DEFAULT_PROFILE;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return DEFAULT_PROFILE;
    }
    if (trimmed.contains(",")) {
      String[] parts = trimmed.split(",");
      for (String part : parts) {
        if (DEFAULT_PROFILE.equalsIgnoreCase(part.trim())) {
          return DEFAULT_PROFILE;
        }
      }
      for (String part : parts) {
        String p = part.trim().toLowerCase(Locale.ROOT);
        if ("dev".equals(p) || "test".equals(p)) {
          return p;
        }
      }
      return parts[0].trim().toLowerCase(Locale.ROOT);
    }
    return trimmed.toLowerCase(Locale.ROOT);
  }

  private static @Nullable Properties getProfileProperties(String profile) {
    return profilePropertiesCache.computeIfAbsent(
        profile, WebTransportConfig::loadProfileProperties);
  }

  private static @Nullable Properties loadProfileProperties(String profile) {
    if ("test".equalsIgnoreCase(profile)) {
      ensureTestCertificatesIfPresent();
    }
    String filename = "webtransport-" + profile + ".properties";
    Properties props = new Properties();
    File localFile = new File(filename);
    if (localFile.exists()) {
      try (InputStream in = new FileInputStream(localFile)) {
        props.load(in);
        logger.info(
            "📡 Loaded profile configuration from local filesystem: {}",
            localFile.getAbsolutePath());
        return props;
      } catch (Exception e) {
        logger.error("❌ Failed to load local profile properties file '{}'", filename, e);
      }
    }
    try (InputStream in =
        WebTransportConfig.class.getClassLoader().getResourceAsStream(filename)) {
      if (in != null) {
        props.load(in);
        logger.info("📡 Loaded profile configuration from classpath resources: {}", filename);
        return props;
      }
    } catch (Exception e) {
      logger.error("❌ Failed to load profile properties from classpath: {}", filename, e);
    }
    return null;
  }

  private static void ensureTestCertificatesIfPresent() {
    try {
      Class<?> testCertClass = Class.forName("io.github.webtransport4j.test.TestCertificates");
      testCertClass.getMethod("ensureTestCertificatesExist").invoke(null);
    } catch (ClassNotFoundException ignored) {
      // TestCertificates is only present on the test classpath
    } catch (ReflectiveOperationException | LinkageError | SecurityException e) {
      logger.error("Failed to ensure test certificates exist", e);
    }
  }

  private static long evaluateExpression(@NonNull String val) {
    val = val.trim();
    if (val.contains("*")) {
      String[] parts = val.split("\\*");
      long result = 1;
      for (String part : parts) {
        result *= Long.parseLong(part.trim());
      }
      return result;
    }
    return Long.parseLong(val);
  }

  /** Returns the int. */
  public static int getInt(@NonNull String key, int defaultValue) {
    return getInt(dynamicProperties, programmaticProperties, key, defaultValue);
  }

  private static int getInt(Properties properties, String key, int defaultValue) {
    return getInt(properties, programmaticProperties, key, defaultValue);
  }

  private static int getInt(
      Properties properties, Properties programmatic, String key, int defaultValue) {
    String val = getVal(properties, programmatic, key, null);
    if (val == null) {
      return defaultValue;
    }
    try {
      return (int) evaluateExpression(val);
    } catch (Exception e) {
      logger.warn(
          "⚠️ Failed to parse int value for key '{}': {}. Using default: {}",
          key,
          val,
          defaultValue,
          e);
      return defaultValue;
    }
  }

  /** Returns the long. */
  public static long getLong(@NonNull String key, long defaultValue) {
    return getLong(dynamicProperties, programmaticProperties, key, defaultValue);
  }

  private static long getLong(Properties properties, String key, long defaultValue) {
    return getLong(properties, programmaticProperties, key, defaultValue);
  }

  private static long getLong(
      Properties properties, Properties programmatic, String key, long defaultValue) {
    String val = getVal(properties, programmatic, key, null);
    if (val == null) {
      return defaultValue;
    }
    try {
      return evaluateExpression(val);
    } catch (Exception e) {
      logger.warn(
          "⚠️ Failed to parse long value for key '{}': {}. Using default: {}",
          key,
          val,
          defaultValue,
          e);
      return defaultValue;
    }
  }

  public static boolean getBoolean(@NonNull String key, boolean defaultValue) {
    return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
  }

  /**
   * Programmatically sets a configuration property at runtime. Properties set via this method
   * override file defaults.
   *
   * @param key the property key (non-null)
   * @param value the property value (non-null)
   */
  public static void setProperty(@NonNull String key, @NonNull String value) {
    EventLoopSafety.requireBlockingAllowed();
    synchronized (revision) {
      Properties updated = new Properties();
      updated.putAll(programmaticProperties);
      updated.setProperty(key, value);
      revision.incrementAndGet();
      programmaticProperties = updated;
    }
  }

  /**
   * Programmatically removes a configuration property at runtime.
   *
   * @param key the property key to remove (non-null)
   */
  public static void removeProperty(@NonNull String key) {
    EventLoopSafety.requireBlockingAllowed();
    synchronized (revision) {
      Properties updated = new Properties();
      updated.putAll(programmaticProperties);
      updated.remove(key);
      revision.incrementAndGet();
      programmaticProperties = updated;
    }
  }

  /**
   * Programmatically clears all configuration properties set via {@link #setProperty(String, String)}.
   */
  public static void clearProgrammaticProperties() {
    EventLoopSafety.requireBlockingAllowed();
    synchronized (revision) {
      programmaticProperties = new Properties();
      revision.incrementAndGet();
    }
  }

  /**
   * Reloads dynamic configuration properties.
   *
   * <p>Parses the dynamic properties file and checks {@code equals()} against active
   * {@code dynamicProperties}. If dynamic properties are identical (e.g. untouched or only
   * comments/formatting changed), skips publication and avoids rebuilding consumer structures.
   *
   * @return {@code true} if dynamic properties semantically changed and were published;
   *         {@code false} otherwise.
   */
  public static boolean reload() {
    EventLoopSafety.requireBlockingAllowed();
    final long token;
    synchronized (revision) {
      token = revision.incrementAndGet();
    }
    profilePropertiesCache.clear();

    Properties newDynamic = new Properties();
    loadDynamicConfig(newDynamic);

    boolean unchanged = newDynamic.equals(dynamicProperties);
    if (unchanged) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "📡 Dynamic config parsed properties are identical; skipping publication.");
      }
      return false;
    }

    synchronized (revision) {
      if (token != revision.get()) {
        return false;
      }
      dynamicProperties = newDynamic;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("📡 Dynamic config reloaded: {} dynamic properties active.", newDynamic.size());
    }
    logger.info("📡 Dynamic config reloaded successfully.");
    return true;
  }

  /**
   * Forces a reload of dynamic configuration properties.
   *
   * @return {@code true} if dynamic properties semantically changed and were published;
   *         {@code false} otherwise.
   */
  public static boolean forceReload() {
    return reload();
  }
}

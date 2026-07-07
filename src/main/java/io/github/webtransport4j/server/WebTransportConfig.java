package io.github.webtransport4j.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configuration utility for WebTransport server settings. */
public class WebTransportConfig {

  private static final Logger logger = LoggerFactory.getLogger(WebTransportConfig.class);

  private static final Properties staticProperties = new Properties();
  private static volatile Properties dynamicProperties = new Properties();

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
        logger.info("📡 Loaded static configuration from local filesystem: {}", localFile.getAbsolutePath());
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
    File localFile = new File("webtransport-dynamic.properties");
    if (localFile.exists()) {
      try (InputStream in = new FileInputStream(localFile)) {
        target.load(in);
        logger.info("📡 Loaded dynamic configuration from local filesystem: {}", localFile.getAbsolutePath());
        return;
      } catch (Exception e) {
        logger.error("❌ Failed to load local dynamic properties file, falling back", e);
      }
    }
    try (InputStream in =
        WebTransportConfig.class.getClassLoader().getResourceAsStream("webtransport-dynamic.properties")) {
      if (in != null) {
        target.load(in);
        logger.info("📡 Loaded default dynamic configuration from classpath resources");
        return;
      }
    } catch (Exception e) {
      logger.error("❌ Failed to load dynamic properties from classpath", e);
    }

    // Fallback: Copy dynamic keys from the static properties
    logger.debug("ℹ️ webtransport-dynamic.properties not found. Dynamic reloads will default to static/default properties.");
    for (String key : staticProperties.stringPropertyNames()) {
      if (isDynamicKey(key)) {
        target.setProperty(key, staticProperties.getProperty(key));
      }
    }
  }

  private static boolean isDynamicKey(String key) {
    return key.startsWith("webtransport4j.server.ratelimit.")
        || key.startsWith("webtransport4j.webtransport.flowcontrol.")
        || key.startsWith("webtransport4j.session.resumption.");
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

  private static String getVal(String key, String defaultVal) {
    // 1. Check System Properties (-Dserver.port=...)
    String value = System.getProperty(key);
    if (value != null) {
      return value;
    }
    // 2. Check Environment Variables (SERVER_PORT=...)
    String envKey = key.toUpperCase().replace('.', '_');
    value = System.getenv(envKey);
    if (value != null) {
      return value;
    }
    // 3. Check dynamic properties
    value = dynamicProperties.getProperty(key);
    if (value != null) {
      return value;
    }
    // 4. Fallback to static properties file
    return staticProperties.getProperty(key, defaultVal);
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
    String val = get(key, null);
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
    String val = get(key, null);
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
   * Reloads dynamic configuration properties.
   */
  public static boolean reload() {
    Properties newDynamic = new Properties();
    loadDynamicConfig(newDynamic);
    if (newDynamic.equals(dynamicProperties)) {
      return false;
    }
    dynamicProperties = newDynamic;
    logger.info("📡 Dynamic config reloaded successfully.");
    return true;
  }
}

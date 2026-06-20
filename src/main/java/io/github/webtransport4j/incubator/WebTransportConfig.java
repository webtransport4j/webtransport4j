package io.github.webtransport4j.incubator;

import io.netty.util.AttributeKey;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

public class WebTransportConfig {
  private static final Logger logger = Logger.getLogger(WebTransportConfig.class.getName());
  private static final Properties properties = new Properties();

  private WebTransportConfig() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated");
  }

  public static final AttributeKey<Long> LOCAL_SETTINGS_MAX_STREAMS_UNI =
      AttributeKey.valueOf("LOCAL_SETTINGS_MAX_STREAMS_UNI");
  public static final AttributeKey<Long> LOCAL_SETTINGS_MAX_STREAMS_BIDI =
      AttributeKey.valueOf("LOCAL_SETTINGS_MAX_STREAMS_BIDI");
  public static final AttributeKey<Long> LOCAL_SETTINGS_MAX_DATA =
      AttributeKey.valueOf("LOCAL_SETTINGS_MAX_DATA");

  public static final AttributeKey<Long> PEER_SETTINGS_MAX_STREAMS_UNI =
      AttributeKey.valueOf("PEER_SETTINGS_MAX_STREAMS_UNI");
  public static final AttributeKey<Long> PEER_SETTINGS_MAX_STREAMS_BIDI =
      AttributeKey.valueOf("PEER_SETTINGS_MAX_STREAMS_BIDI");
  public static final AttributeKey<Long> PEER_SETTINGS_MAX_DATA =
      AttributeKey.valueOf("PEER_SETTINGS_MAX_DATA");

  static {
    try (InputStream in =
        WebTransportConfig.class.getClassLoader().getResourceAsStream("webtransport.properties")) {
      if (in != null) {
        properties.load(in);
        logger.info("📡 Loaded default configuration from webtransport.properties");
      } else {
        logger.warn("⚠️ webtransport.properties not found in resources. Using fallback defaults.");
      }
    } catch (Exception e) {
      logger.error("❌ Failed to load properties", e);
    }
  }

  /**
   * Resolves configuration key with precedence: 1. Java System Property (-Dkey=value) 2.
   * Environment Variable (ENV_KEY_NAME) 3. properties file default value
   */
  public static String get(String key, String defaultValue) {
    // 1. Check System Properties (-Dserver.port=...)
    String value = System.getProperty(key);
    if (value != null) return value;

    // 2. Check Environment Variables (SERVER_PORT=...)
    String envKey = key.toUpperCase().replace('.', '_');
    value = System.getenv(envKey);
    if (value != null) return value;

    // 3. Fallback to properties file
    return properties.getProperty(key, defaultValue);
  }

  private static long evaluateExpression(String val) {
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

  public static int getInt(String key, int defaultValue) {
    String val = get(key, null);
    if (val == null) {
      return defaultValue;
    }
    try {
      return (int) evaluateExpression(val);
    } catch (Exception e) {
      logger.warn(
          "⚠️ Failed to parse int value for key '"
              + key
              + "': "
              + val
              + ". Using default: "
              + defaultValue,
          e);
      return defaultValue;
    }
  }

  public static long getLong(String key, long defaultValue) {
    String val = get(key, null);
    if (val == null) {
      return defaultValue;
    }
    try {
      return evaluateExpression(val);
    } catch (Exception e) {
      logger.warn(
          "⚠️ Failed to parse long value for key '"
              + key
              + "': "
              + val
              + ". Using default: "
              + defaultValue,
          e);
      return defaultValue;
    }
  }

  public static boolean getBoolean(String key, boolean defaultValue) {
    return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
  }
}

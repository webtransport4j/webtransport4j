package io.github.webtransport4j.incubator;

import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

public class WebTransportConfig {
    private static final Logger logger = Logger.getLogger(WebTransportConfig.class.getName());
    private static final Properties properties = new Properties();

    static {
        try (InputStream in = WebTransportConfig.class.getClassLoader()
                .getResourceAsStream("webtransport.properties")) {
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
     * Resolves configuration key with precedence:
     * 1. Java System Property (-Dkey=value)
     * 2. Environment Variable (ENV_KEY_NAME)
     * 3. properties file default value
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

    public static int getInt(String key, int defaultValue) {
        return Integer.parseInt(get(key, String.valueOf(defaultValue)));
    }

    public static long getLong(String key, long defaultValue) {
        return Long.parseLong(get(key, String.valueOf(defaultValue)));
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }
}

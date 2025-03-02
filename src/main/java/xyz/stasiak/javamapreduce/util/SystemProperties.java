package xyz.stasiak.javamapreduce.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class SystemProperties {
    private static final Logger LOGGER = Logger.getLogger(SystemProperties.class.getName());
    private static final Properties properties = new Properties();

    static {
        try (var loggingProperties = SystemProperties.class.getClassLoader().getResourceAsStream("logging.properties");
                var applicationProperties = SystemProperties.class.getClassLoader()
                        .getResourceAsStream("application.properties")) {
            LogManager.getLogManager().readConfiguration(loggingProperties);
            properties.load(applicationProperties);
        } catch (IOException e) {
            LoggingUtil.logSevere(LOGGER, SystemProperties.class, "Could not load configuration", e);
            throw new IllegalStateException("Could not load configuration", e);
        }
    }

    public static String getNodeDirectory() {
        return getProperty("node.directory");
    }

    public static String getPublicDirectory() {
        return getProperty("public.directory");
    }

    public static String getRmiPort() {
        return getProperty("rmi.port");
    }

    public static String getNodeAddress() {
        return getProperty("node.address");
    }

    public static List<String> getKnownNodes() {
        return Arrays.stream(getProperty("known.nodes").split(","))
                .map(String::trim)
                .toList();
    }

    private static String getProperty(String key) {
        var property = System.getProperty(key, properties.getProperty(key));
        if (property == null) {
            throw new IllegalStateException("Property %s not set in application.properties".formatted(key));
        }
        return property;
    }
}

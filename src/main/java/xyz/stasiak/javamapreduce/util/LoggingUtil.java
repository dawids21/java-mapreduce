package xyz.stasiak.javamapreduce.util;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggingUtil {
    private LoggingUtil() {
    }

    public static void logInfo(Logger logger, Class<?> clazz, String message) {
        logger.info("() [%s] %s".formatted(clazz.getSimpleName(), message));
    }

    public static void logInfo(Logger logger, int processingId, Class<?> clazz, String message) {
        logger.info("(%d) [%s] %s".formatted(processingId, clazz.getSimpleName(), message));
    }

    public static void logWarning(Logger logger, Class<?> clazz, String message) {
        logger.warning("() [%s] %s".formatted(clazz.getSimpleName(), message));
    }

    public static void logWarning(Logger logger, int processingId, Class<?> clazz, String message) {
        logger.warning("(%d) [%s] %s".formatted(processingId, clazz.getSimpleName(), message));
    }

    public static void logWarning(Logger logger, Class<?> clazz, String message, Throwable throwable) {
        logger.log(Level.WARNING, "() [%s] %s".formatted(clazz.getSimpleName(), message), throwable);
    }

    public static void logWarning(Logger logger, int processingId, Class<?> clazz, String message,
            Throwable throwable) {
        logger.log(Level.WARNING, "(%d) [%s] %s".formatted(processingId, clazz.getSimpleName(), message), throwable);
    }

    public static void logSevere(Logger logger, Class<?> clazz, String message) {
        logger.severe("() [%s] %s".formatted(clazz.getSimpleName(), message));
    }

    public static void logSevere(Logger logger, int processingId, Class<?> clazz, String message) {
        logger.severe("(%d) [%s] %s".formatted(processingId, clazz.getSimpleName(), message));
    }

    public static void logSevere(Logger logger, Class<?> clazz, String message, Throwable throwable) {
        logger.log(Level.SEVERE, "() [%s] %s".formatted(clazz.getSimpleName(), message), throwable);
    }

    public static void logSevere(Logger logger, int processingId, Class<?> clazz, String message,
            Throwable throwable) {
        logger.log(Level.SEVERE, "(%d) [%s] %s".formatted(processingId, clazz.getSimpleName(), message), throwable);
    }
}
package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;

record MapResult(
        boolean success,
        Path outputFile,
        String error,
        int retryCount) {

    static MapResult success(Path outputFile, int retryCount) {
        return new MapResult(true, outputFile, null, retryCount);
    }

    static MapResult failure(String error, int retryCount) {
        return new MapResult(false, null, error, retryCount);
    }

    boolean requiresRetry() {
        return !success && error != null;
    }
}
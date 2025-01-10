package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;

record MapResult(
        boolean success,
        String error,
        Path outputFile) {

    static MapResult success(Path outputFile) {
        return new MapResult(true, null, outputFile);
    }

    static MapResult failure(String error) {
        return new MapResult(false, error, null);
    }

    boolean requiresRetry() {
        return !success && error != null;
    }
}
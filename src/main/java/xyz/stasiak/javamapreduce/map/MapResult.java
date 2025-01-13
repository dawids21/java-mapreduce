package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;

record MapResult(
        Throwable error,
        Path outputFile) {

    static MapResult success(Path outputFile) {
        return new MapResult(null, outputFile);
    }

    static MapResult failure(Throwable error) {
        return new MapResult(error, null);
    }

    boolean isSuccess() {
        return error == null;
    }

    boolean requiresRetry() {
        return !isSuccess();
    }
}

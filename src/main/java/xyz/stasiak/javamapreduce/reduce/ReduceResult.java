package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Path;

record ReduceResult(
        String error,
        Path outputFile) {

    static ReduceResult success(Path outputFile) {
        return new ReduceResult(null, outputFile);
    }

    static ReduceResult failure(String error) {
        return new ReduceResult(error, null);
    }

    boolean isSuccess() {
        return error == null;
    }

    boolean requiresRetry() {
        return !isSuccess();
    }
}
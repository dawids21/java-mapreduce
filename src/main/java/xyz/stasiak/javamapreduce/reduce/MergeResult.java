package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Path;

record MergeResult(
        Throwable error,
        Path outputFile) {

    static MergeResult success(Path outputFile) {
        return new MergeResult(null, outputFile);
    }

    static MergeResult failure(Throwable error) {
        return new MergeResult(error, null);
    }

    boolean isSuccess() {
        return error == null;
    }

    boolean requiresRetry() {
        return !isSuccess();
    }
}
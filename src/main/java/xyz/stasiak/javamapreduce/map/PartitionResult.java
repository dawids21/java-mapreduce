package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.List;

record PartitionResult(
        Throwable error,
        List<Path> outputFiles) {

    static PartitionResult success(List<Path> outputFiles) {
        return new PartitionResult(null, outputFiles);
    }

    static PartitionResult failure(Throwable error) {
        return new PartitionResult(error, List.of());
    }

    boolean isSuccess() {
        return error == null;
    }

    boolean requiresRetry() {
        return !isSuccess();
    }
}

package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.List;

record PartitionResult(
        String error,
        List<Path> outputFiles) {

    static PartitionResult success(List<Path> outputFiles) {
        return new PartitionResult(null, outputFiles);
    }

    static PartitionResult failure(String error) {
        return new PartitionResult(error, List.of());
    }

    boolean isSuccess() {
        return error == null;
    }

    boolean requiresRetry() {
        return !isSuccess();
    }
}

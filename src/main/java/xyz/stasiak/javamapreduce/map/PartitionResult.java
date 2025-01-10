package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.List;

record PartitionResult(
        boolean success,
        String error,
        List<Path> outputFiles) {

    static PartitionResult success(List<Path> outputFiles) {
        return new PartitionResult(true, null, outputFiles);
    }

    static PartitionResult failure(String error) {
        return new PartitionResult(false, error, List.of());
    }

    boolean requiresRetry() {
        return !success && error != null;
    }
}

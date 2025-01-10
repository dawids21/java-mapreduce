package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.function.Function;

record PartitionTask(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        Function<String, Integer> partitionFunction,
        int maxRetries) {

    static PartitionTask create(int processingId, Path inputFile, Path outputDirectory,
            Function<String, Integer> partitionFunction) {
        return new PartitionTask(processingId, inputFile, outputDirectory, partitionFunction, 3);
    }

    PartitionTask withIncrementedRetries() {
        return new PartitionTask(processingId, inputFile, outputDirectory, partitionFunction, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.function.Function;

import xyz.stasiak.javamapreduce.processing.CancellationToken;

record PartitionTask(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        Function<String, Integer> partitionFunction,
        CancellationToken cancellationToken,
        int maxRetries) {

    static PartitionTask create(int processingId, Path inputFile, Path outputDirectory,
            Function<String, Integer> partitionFunction, CancellationToken cancellationToken) {
        return new PartitionTask(processingId, inputFile, outputDirectory, partitionFunction, cancellationToken, 3);
    }

    PartitionTask withIncrementedRetries() {
        return new PartitionTask(processingId, inputFile, outputDirectory, partitionFunction, cancellationToken,
                maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
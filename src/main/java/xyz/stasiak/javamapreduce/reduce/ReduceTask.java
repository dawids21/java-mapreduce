package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Path;

import xyz.stasiak.javamapreduce.processing.CancellationToken;

record ReduceTask(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        Reducer reducer,
        CancellationToken cancellationToken,
        int maxRetries) {

    static ReduceTask create(int processingId, Path inputFile, Path outputDirectory, Reducer reducer,
            CancellationToken cancellationToken) {
        return new ReduceTask(processingId, inputFile, outputDirectory, reducer, cancellationToken, 3);
    }

    ReduceTask withIncrementedRetries() {
        return new ReduceTask(processingId, inputFile, outputDirectory, reducer, cancellationToken, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
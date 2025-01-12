package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Path;

record ReduceTask(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        Reducer reducer,
        int maxRetries) {

    static ReduceTask create(int processingId, Path inputFile, Path outputDirectory, Reducer reducer) {
        return new ReduceTask(processingId, inputFile, outputDirectory, reducer, 3);
    }

    ReduceTask withIncrementedRetries() {
        return new ReduceTask(processingId, inputFile, outputDirectory, reducer, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
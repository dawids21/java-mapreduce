package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;

import xyz.stasiak.javamapreduce.rmi.CancellationToken;

record MapTask(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        Mapper mapper,
        CancellationToken cancellationToken,
        int maxRetries) {

    static MapTask create(int processingId, Path inputFile, Path outputDirectory, Mapper mapper,
            CancellationToken cancellationToken) {
        return new MapTask(processingId, inputFile, outputDirectory, mapper, cancellationToken, 3);
    }

    MapTask withIncrementedRetries() {
        return new MapTask(processingId, inputFile, outputDirectory, mapper, cancellationToken, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
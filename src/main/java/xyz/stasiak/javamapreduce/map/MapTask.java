package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;

record MapTask(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        Mapper mapper,
        int maxRetries) {

    static MapTask create(int processingId, Path inputFile, Path outputDirectory, Mapper mapper) {
        return new MapTask(processingId, inputFile, outputDirectory, mapper, 3);
    }

    MapTask withIncrementedRetries() {
        return new MapTask(processingId, inputFile, outputDirectory, mapper, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
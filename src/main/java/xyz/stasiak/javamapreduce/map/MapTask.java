package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;

record MapTask(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        String mapperClassName,
        int maxRetries,
        String nodeAddress) {

    static MapTask create(int processingId, Path inputFile, Path outputDirectory, String mapperClassName,
            String nodeAddress) {
        return new MapTask(processingId, inputFile, outputDirectory, mapperClassName, 3, nodeAddress);
    }

    MapTask withIncrementedRetries() {
        return new MapTask(processingId, inputFile, outputDirectory, mapperClassName, maxRetries - 1, nodeAddress);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;

record MapTask<K, V>(
        int processingId,
        Path inputFile,
        Path outputDirectory,
        Mapper<K, V> mapper,
        int maxRetries) {

    static <K, V> MapTask<K, V> create(int processingId, Path inputFile, Path outputDirectory, Mapper<K, V> mapper) {
        return new MapTask<>(processingId, inputFile, outputDirectory, mapper, 3);
    }

    MapTask<K, V> withIncrementedRetries() {
        return new MapTask<>(processingId, inputFile, outputDirectory, mapper, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
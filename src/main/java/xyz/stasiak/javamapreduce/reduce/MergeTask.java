package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Path;
import java.util.List;

record MergeTask(
        int processingId,
        int partitionId,
        List<Path> inputFiles,
        Path outputDirectory,
        int maxRetries) {

    static MergeTask create(int processingId, int partitionId, List<Path> inputFiles, Path outputDirectory) {
        return new MergeTask(processingId, partitionId, inputFiles, outputDirectory, 3);
    }

    MergeTask withIncrementedRetries() {
        return new MergeTask(processingId, partitionId, inputFiles, outputDirectory, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
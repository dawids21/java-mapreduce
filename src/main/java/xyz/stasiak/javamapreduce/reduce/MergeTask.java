package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Path;
import java.util.List;

import xyz.stasiak.javamapreduce.rmi.CancellationToken;

record MergeTask(
        int processingId,
        int partitionId,
        List<Path> inputFiles,
        Path outputDirectory,
        CancellationToken cancellationToken,
        int maxRetries) {

    static MergeTask create(int processingId, int partitionId, List<Path> inputFiles, Path outputDirectory,
            CancellationToken cancellationToken) {
        return new MergeTask(processingId, partitionId, inputFiles, outputDirectory, cancellationToken, 3);
    }

    MergeTask withIncrementedRetries() {
        return new MergeTask(processingId, partitionId, inputFiles, outputDirectory, cancellationToken, maxRetries - 1);
    }

    boolean canRetry() {
        return maxRetries > 0;
    }
}
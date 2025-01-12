package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.files.FileManager;

public class ReducePhaseCoordinator {
    private static final Logger LOGGER = Logger.getLogger(ReducePhaseCoordinator.class.getName());
    private static final int MAX_RETRIES = 1;

    private final int processingId;
    private final String reducerClassName;
    private final List<Integer> partitionAssignments;
    private final ExecutorService executor;

    public ReducePhaseCoordinator(int processingId, String reducerClassName, List<Integer> partitionAssignments) {
        this.processingId = processingId;
        this.reducerClassName = reducerClassName;
        this.partitionAssignments = partitionAssignments;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public ReducePhaseResult execute() {
        LOGGER.info("(%d) [%s] Starting reduce phase with %d partitions".formatted(
                processingId, ReducePhaseCoordinator.class.getSimpleName(), partitionAssignments.size()));

        var attempt = 0;

        try {
            while (attempt <= MAX_RETRIES) {
                var result = processPartitions(partitionAssignments);

                if (result.failedPartitions().isEmpty()) {
                    LOGGER.info("(%d) [%s] Reduce phase completed successfully".formatted(
                            processingId, ReducePhaseCoordinator.class.getSimpleName()));
                    return new ReducePhaseResult(result.processedCount());
                }

                LOGGER.info("(%d) [%s] Retrying reduce phase for failed partitions".formatted(
                        processingId, ReducePhaseCoordinator.class.getSimpleName()));
                attempt++;
            }

            LOGGER.severe("(%d) [%s] Reduce phase failed".formatted(
                    processingId, ReducePhaseCoordinator.class.getSimpleName()));
            throw new RuntimeException("Reduce phase failed");
        } finally {
            executor.close();
        }
    }

    private record ProcessingResult(int processedCount, List<Integer> failedPartitions) {
    }

    private ProcessingResult processPartitions(List<Integer> partitionsToProcess) {
        var futures = new ArrayList<Future<MergeResult>>();
        var mergeFilesDirectory = FileManager.getMergeFilesDirectory(processingId);

        for (var partitionId : partitionsToProcess) {
            var partitionDirectory = FileManager.getPartitionDirectory(processingId, partitionId);
            try {
                var inputFiles = Files.list(partitionDirectory)
                        .filter(Files::isRegularFile)
                        .toList();

                if (!inputFiles.isEmpty()) {
                    var mergeTask = MergeTask.create(processingId, partitionId, inputFiles, mergeFilesDirectory);
                    var mergeTaskExecutor = new MergeTaskExecutor(mergeTask);
                    futures.add(executor.submit(mergeTaskExecutor::execute));
                }
            } catch (Exception e) {
                LOGGER.severe("(%d) [%s] Error listing files in partition directory %d: %s".formatted(
                        processingId, ReducePhaseCoordinator.class.getSimpleName(), partitionId, e.getMessage()));
            }
        }

        int processedPartitions = 0;
        var failedPartitions = new ArrayList<Integer>();

        for (var i = 0; i < futures.size(); i++) {
            try {
                var result = futures.get(i).get();
                if (!result.isSuccess()) {
                    failedPartitions.add(partitionsToProcess.get(i));
                } else {
                    processedPartitions++;
                }
            } catch (Exception e) {
                LOGGER.severe("(%d) [%s] Error executing merge task: %s".formatted(
                        processingId, ReducePhaseCoordinator.class.getSimpleName(), e.getMessage()));
                failedPartitions.add(partitionsToProcess.get(i));
            }
        }

        return new ProcessingResult(processedPartitions, failedPartitions);
    }
}
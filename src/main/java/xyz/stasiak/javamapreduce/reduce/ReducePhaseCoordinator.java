package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Files;
import java.nio.file.Path;
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
    private final Path outputDirectory;
    private final ExecutorService executor;

    public ReducePhaseCoordinator(int processingId, String reducerClassName, List<Integer> partitionAssignments,
            Path outputDirectory) {
        this.processingId = processingId;
        this.reducerClassName = reducerClassName;
        this.partitionAssignments = partitionAssignments;
        this.outputDirectory = outputDirectory;
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
        var reducer = ReducerFactory.createReducer(reducerClassName);
        var futures = new ArrayList<Future<MergeReduceResult>>();
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
                    var reduceTask = ReduceTask.create(processingId,
                            mergeFilesDirectory.resolve(String.valueOf(partitionId)),
                            outputDirectory, reducer);
                    var reduceTaskExecutor = new ReduceTaskExecutor(reduceTask);

                    futures.add(executor.submit(() -> {
                        var mergeResult = mergeTaskExecutor.execute();
                        if (!mergeResult.isSuccess()) {
                            return MergeReduceResult.failure(mergeResult.error());
                        }
                        var reduceResult = reduceTaskExecutor.execute();
                        if (!reduceResult.isSuccess()) {
                            return MergeReduceResult.failure(reduceResult.error());
                        }
                        return MergeReduceResult.success();
                    }));
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
                LOGGER.severe("(%d) [%s] Error executing reduce task: %s".formatted(
                        processingId, ReducePhaseCoordinator.class.getSimpleName(), e.getMessage()));
                failedPartitions.add(partitionsToProcess.get(i));
            }
        }

        return new ProcessingResult(processedPartitions, failedPartitions);
    }
}
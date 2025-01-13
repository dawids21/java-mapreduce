package xyz.stasiak.javamapreduce.reduce;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.rmi.ProcessingException;
import xyz.stasiak.javamapreduce.util.FilesUtil;
import xyz.stasiak.javamapreduce.util.LoggingUtil;

public class ReducePhaseCoordinator {
    private static final Logger LOGGER = Logger.getLogger(ReducePhaseCoordinator.class.getName());
    private static final int MAX_RETRIES = 1;

    private final int processingId;
    private final String reducerClassName;
    private final List<Integer> partitionAssignments;
    private final Map<Integer, List<Path>> partitionFiles;
    private final Path outputDirectory;
    private final ExecutorService executor;

    public ReducePhaseCoordinator(int processingId, String reducerClassName, List<Integer> partitionAssignments,
            Map<Integer, List<Path>> partitionFiles, Path outputDirectory) {
        this.processingId = processingId;
        this.reducerClassName = reducerClassName;
        this.partitionAssignments = partitionAssignments;
        this.partitionFiles = partitionFiles;
        this.outputDirectory = outputDirectory;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public ReducePhaseResult execute() {
        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "Starting reduce phase with partitions: %s".formatted(partitionAssignments));

        var attempt = 0;

        try {
            while (attempt <= MAX_RETRIES) {
                var result = processPartitions(partitionAssignments, partitionFiles);

                if (result.failedPartitions().isEmpty()) {
                    LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                            "Reduce phase completed successfully");
                    return new ReducePhaseResult(result.processedCount());
                }

                LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                        "Retrying reduce phase for failed partitions");
                attempt++;
            }

            LoggingUtil.logSevere(LOGGER, processingId, getClass(), "Reduce phase failed");
            throw new ProcessingException("Reduce phase failed");
        } finally {
            executor.close();
        }
    }

    private record ProcessingResult(int processedCount, List<Integer> failedPartitions) {
    }

    private ProcessingResult processPartitions(List<Integer> partitionsToProcess,
            Map<Integer, List<Path>> partitionFiles) {
        var reducer = ReducerFactory.createReducer(reducerClassName);
        var futures = new ArrayList<Future<MergeReduceResult>>();
        var mergeFilesDirectory = FilesUtil.getMergeFilesDirectory(processingId);

        for (var partitionId : partitionsToProcess) {
            var partitionDirectory = FilesUtil.getPartitionDirectory(processingId, partitionId);
            var inputFiles = partitionFiles.get(partitionId);
            var inputPaths = inputFiles.stream()
                    .map(partitionDirectory::resolve)
                    .toList();

            if (!inputFiles.isEmpty()) {
                var mergeTask = MergeTask.create(processingId, partitionId, inputPaths, mergeFilesDirectory);
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
                LoggingUtil.logSevere(LOGGER, processingId, getClass(), "Error executing reduce task", e);
                failedPartitions.add(partitionsToProcess.get(i));
            }
        }

        return new ProcessingResult(processedPartitions, failedPartitions);
    }
}
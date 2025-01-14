package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.rmi.ProcessingException;
import xyz.stasiak.javamapreduce.util.FilesUtil;
import xyz.stasiak.javamapreduce.util.LoggingUtil;

public class MapPhaseCoordinator {
    private static final Logger LOGGER = Logger.getLogger(MapPhaseCoordinator.class.getName());
    private static final int MAX_RETRIES = 1;
    private final int processingId;
    private final String mapperClassName;
    private final Path inputDirectory;
    private final List<Path> files;
    private final Function<String, Integer> partitionFunction;
    private final ExecutorService executor;

    public MapPhaseCoordinator(int processingId, String mapperClassName, String inputDirectory, List<String> files,
            Function<String, Integer> partitionFunction) {
        this.processingId = processingId;
        this.mapperClassName = mapperClassName;
        this.inputDirectory = Path.of(inputDirectory);
        this.files = files.stream()
                .map(Path::of)
                .toList();
        this.partitionFunction = partitionFunction;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public MapPhaseResult execute() {
        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "Starting map phase with files: %s".formatted(files));

        var attempt = 0;

        try {
            while (attempt <= MAX_RETRIES) {
                var result = processFiles(files);

                if (result.failedFiles().isEmpty()) {
                    LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                            "Map phase completed successfully");
                    return new MapPhaseResult(result.processedCount());
                }

                LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                        "Retrying map phase for all files");
                attempt++;
            }

            LoggingUtil.logSevere(LOGGER, processingId, getClass(), "Map phase failed");
            throw new ProcessingException("Map phase failed");
        } finally {
            executor.close();
        }
    }

    private record ProcessingResult(int processedCount, List<Path> failedFiles) {
    }

    private ProcessingResult processFiles(List<Path> filesToProcess) {
        var mapper = MapperFactory.createMapper(mapperClassName);
        var mapFilesDirectory = FilesUtil.getMapFilesDirectory(processingId);
        var partitionFilesDirectory = FilesUtil.getPartitionFilesDirectory(processingId);
        var futures = new ArrayList<Future<MapPartitionResult>>();

        for (var file : filesToProcess) {
            var mapTask = MapTask.create(processingId, inputDirectory.resolve(file), mapFilesDirectory, mapper);
            var mapTaskExecutor = new MapTaskExecutor(mapTask);
            var partitionTask = PartitionTask.create(processingId, mapFilesDirectory.resolve(file),
                    partitionFilesDirectory, partitionFunction);
            var partitionTaskExecutor = new PartitionTaskExecutor(partitionTask);
            futures.add(this.executor.submit(() -> {
                var mapResult = mapTaskExecutor.execute();
                if (mapResult.requiresRetry()) {
                    return MapPartitionResult.failure(mapResult.error());
                }
                var partitionResult = partitionTaskExecutor.execute();
                if (partitionResult.requiresRetry()) {
                    return MapPartitionResult.failure(partitionResult.error());
                }
                return MapPartitionResult.success();
            }));
        }

        int processedFiles = 0;
        var failedFiles = new ArrayList<Path>();

        for (var i = 0; i < futures.size(); i++) {
            try {
                var result = futures.get(i).get();
                if (!result.isSuccess()) {
                    failedFiles.add(filesToProcess.get(i));
                } else {
                    processedFiles++;
                }
            } catch (Exception e) {
                LoggingUtil.logSevere(LOGGER, processingId, getClass(), "Error executing map task", e);
                failedFiles.add(filesToProcess.get(i));
            }
        }

        return new ProcessingResult(processedFiles, failedFiles);
    }
}

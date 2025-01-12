package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.files.FileManager;

public class MapPhaseCoordinator {
    private static final Logger LOGGER = Logger.getLogger(MapPhaseCoordinator.class.getName());
    private static final int MAX_RETRIES = 1;
    private final int processingId;
    private final String mapperClassName;
    private final Path inputDirectory;
    private final List<Path> files;
    private final Function<String, Integer> partitionFunction;
    private final ExecutorService executor;

    public MapPhaseCoordinator(int processingId, String mapperClassName, Path inputDirectory, List<Path> files,
            Function<String, Integer> partitionFunction) {
        this.processingId = processingId;
        this.mapperClassName = mapperClassName;
        this.inputDirectory = inputDirectory;
        this.files = files;
        this.partitionFunction = partitionFunction;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public MapPhaseResult execute() {
        LOGGER.info("(%d) [%s] Starting map phase with %d files".formatted(processingId,
                MapPhaseCoordinator.class.getSimpleName(), files.size()));

        var attempt = 0;

        try {
            while (attempt <= MAX_RETRIES) {
                var result = processFiles(files);

                if (result.failedFiles().isEmpty()) {
                    LOGGER.info("(%d) [%s] Map phase completed successfully".formatted(processingId,
                            MapPhaseCoordinator.class.getSimpleName()));
                    return new MapPhaseResult(result.processedCount());
                }

                LOGGER.info("(%d) [%s] Retrying map phase for all files"
                        .formatted(processingId, MapPhaseCoordinator.class.getSimpleName()));
                attempt++;
            }

            LOGGER.severe(
                    "(%d) [%s] Map phase failed".formatted(processingId, MapPhaseCoordinator.class.getSimpleName()));
            throw new RuntimeException("Map phase failed");
        } finally {
            executor.close();
        }
    }

    private record ProcessingResult(int processedCount, List<Path> failedFiles) {
    }

    private ProcessingResult processFiles(List<Path> filesToProcess) {
        var mapper = MapperFactory.createMapper(mapperClassName);
        var mapFilesDirectory = FileManager.getMapFilesDirectory(processingId);
        var partitionFilesDirectory = FileManager.getPartitionFilesDirectory(processingId);
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
                LOGGER.severe("(%d) [%s] Error executing map task: %s".formatted(processingId,
                        MapPhaseCoordinator.class.getSimpleName(), e.getMessage()));
                failedFiles.add(filesToProcess.get(i));
            }
        }

        return new ProcessingResult(processedFiles, failedFiles);
    }
}

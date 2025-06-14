package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.processing.CancellationToken;
import xyz.stasiak.javamapreduce.processing.ProcessingException;
import xyz.stasiak.javamapreduce.util.FilesUtil;
import xyz.stasiak.javamapreduce.util.LoggingUtil;

public class MapPhaseCoordinator {
    private static final Logger LOGGER = Logger.getLogger(MapPhaseCoordinator.class.getName());
    private static final int MAX_RETRIES = 2;
    private final int processingId;
    private final String mapperClassName;
    private final Path inputDirectory;
    private final List<Path> files;
    private final Function<String, Integer> partitionFunction;
    private final CancellationToken cancellationToken;

    public MapPhaseCoordinator(int processingId, String mapperClassName, String inputDirectory, List<String> files,
            Function<String, Integer> partitionFunction, CancellationToken cancellationToken) {
        this.processingId = processingId;
        this.mapperClassName = mapperClassName;
        this.inputDirectory = Path.of(inputDirectory);
        this.files = files.stream()
                .map(Path::of)
                .toList();
        this.partitionFunction = partitionFunction;
        this.cancellationToken = cancellationToken;
    }

    public MapPhaseResult execute() {
        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "Starting map phase with files: %s".formatted(files));

        var remainingAttempts = MAX_RETRIES;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            while (remainingAttempts > 0) {
                cancellationToken.throwIfCancelled(processingId, "Map phase cancelled");

                var result = processFiles(files, executor);

                if (result.failedFiles().isEmpty()) {
                    LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                            "Map phase completed successfully");
                    return new MapPhaseResult(result.processedFiles().stream()
                            .map(f -> f.getFileName().toString())
                            .toList());
                }

                LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                        "Retrying map phase for all files");
                remainingAttempts--;
            }

            LoggingUtil.logSevere(LOGGER, processingId, getClass(), "Map phase failed");
            throw new ProcessingException("Map phase failed");
        }
    }

    private record ProcessingResult(List<Path> processedFiles, List<Path> failedFiles) {
    }

    private ProcessingResult processFiles(List<Path> filesToProcess, ExecutorService executor) {
        cancellationToken.throwIfCancelled(processingId, "Map phase cancelled");

        var mapper = MapperFactory.createMapper(mapperClassName);
        var mapFilesDirectory = FilesUtil.getMapFilesDirectory(processingId);
        var partitionFilesDirectory = FilesUtil.getPartitionFilesDirectory(processingId);
        var futures = new ArrayList<Future<MapPartitionResult>>();

        for (var file : filesToProcess) {
            cancellationToken.throwIfCancelled(processingId, "Map phase cancelled");

            var mapTask = MapTask.create(processingId, inputDirectory.resolve(file), mapFilesDirectory, mapper,
                    cancellationToken);
            var mapTaskRunner = new MapTaskRunner(mapTask);
            var partitionTask = PartitionTask.create(processingId, mapFilesDirectory.resolve(file),
                    partitionFilesDirectory, partitionFunction, cancellationToken);
            var partitionTaskRunner = new PartitionTaskRunner(partitionTask);
            futures.add(executor.submit(() -> {
                var mapResult = mapTaskRunner.execute();
                if (mapResult.requiresRetry()) {
                    return MapPartitionResult.failure(mapResult.error());
                }
                var partitionResult = partitionTaskRunner.execute();
                if (partitionResult.requiresRetry()) {
                    return MapPartitionResult.failure(partitionResult.error());
                }
                return MapPartitionResult.success();
            }));
        }

        var processedFiles = new ArrayList<Path>();
        var failedFiles = new ArrayList<Path>();

        for (var i = 0; i < futures.size(); i++) {
            try {
                var result = futures.get(i).get();
                if (!result.isSuccess()) {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Error processing file: %s".formatted(filesToProcess.get(i)), result.error());
                    failedFiles.add(filesToProcess.get(i));
                } else {
                    processedFiles.add(filesToProcess.get(i));
                }
            } catch (Exception e) {
                LoggingUtil.logSevere(LOGGER, processingId, getClass(), "Error executing map task", e);
                failedFiles.add(filesToProcess.get(i));
            }
        }

        return new ProcessingResult(processedFiles, failedFiles);
    }
}

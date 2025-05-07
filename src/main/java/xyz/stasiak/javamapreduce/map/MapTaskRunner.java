package xyz.stasiak.javamapreduce.map;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.processing.ProcessingCancelledException;
import xyz.stasiak.javamapreduce.util.FilesUtil;
import xyz.stasiak.javamapreduce.util.LoggingUtil;
import xyz.stasiak.javamapreduce.util.SystemProperties;

class MapTaskRunner {
    private static final Logger LOGGER = Logger.getLogger(MapTaskRunner.class.getName());
    private final MapTask task;

    MapTaskRunner(MapTask task) {
        this.task = task;
    }

    MapResult execute() {
        var currentTask = task;
        MapResult result = null;

        while (currentTask.canRetry()) {
            try {
                currentTask.cancellationToken().throwIfCancelled(currentTask.processingId(), "Map task cancelled");
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(),
                        "Processing file: %s".formatted(currentTask.inputFile()));
                var outputFile = FilesUtil.getMapFilesDirectory(currentTask.processingId())
                        .resolve(currentTask.inputFile().getFileName());
                processFile(currentTask.inputFile(), outputFile);
                result = MapResult.success(outputFile);
                break;
            } catch (ProcessingCancelledException e) {
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(), "Map task cancelled");
                result = MapResult.failure(e);
                break;
            } catch (Exception e) {
                LoggingUtil.logWarning(LOGGER, currentTask.processingId(), getClass(),
                        "Error processing file: %s. Retries left: %d".formatted(currentTask.inputFile(),
                                currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = MapResult.failure(e);
            }
        }

        return result;
    }

    private void processFile(Path inputFile, Path outputFile) throws IOException {
        try (var reader = Files.newBufferedReader(inputFile);
                var outputStream = Files.newOutputStream(outputFile);
                var bufferedOutputStream = new BufferedOutputStream(outputStream, 1024 * 1024);
                var objectWriter = new ObjectOutputStream(bufferedOutputStream)) {
            String line;
            boolean injectException = SystemProperties.injectException();
            while ((line = reader.readLine()) != null) {
                task.cancellationToken().throwIfCancelled(task.processingId(),
                        "Map task cancelled");
                if (injectException) {
                    throw new IOException("Injected exception in map task");
                }
                processLine(line, objectWriter);
            }
        }
    }

    private void processLine(String line, ObjectOutputStream writer) throws IOException {
        var mapper = task.mapper();
        var keyValues = mapper.map(line);
        for (var keyValue : keyValues) {
            writer.writeObject(keyValue);
        }
    }
}
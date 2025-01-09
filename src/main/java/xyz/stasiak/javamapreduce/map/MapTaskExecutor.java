package xyz.stasiak.javamapreduce.map;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.files.FileManager;

class MapTaskExecutor<K, V> {
    private static final Logger LOGGER = Logger.getLogger(MapTaskExecutor.class.getName());
    private final MapTask task;
    private final Mapper<K, V> mapper;

    MapTaskExecutor(MapTask task, Mapper<K, V> mapper) {
        this.task = task;
        this.mapper = mapper;
    }

    MapResult execute() {
        var currentTask = task;
        MapResult result = null;

        while (currentTask.canRetry()) {
            try {
                var outputFile = FileManager.getMapFilesDirectory(currentTask.processingId())
                        .resolve(currentTask.inputFile().getFileName());
                processFile(currentTask.inputFile(), outputFile);
                result = MapResult.success(outputFile, task.maxRetries() - currentTask.maxRetries());
                break;
            } catch (Exception e) {
                LOGGER.warning("Error processing file: %s %s. Retries left: %d"
                        .formatted(currentTask.inputFile(), e.getMessage(), currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = MapResult.failure(e.getMessage(), task.maxRetries() - currentTask.maxRetries());
            }
        }

        return result;
    }

    private void processFile(Path inputFile, Path outputFile) throws IOException {
        try (var reader = Files.newBufferedReader(inputFile);
                var writer = Files.newBufferedWriter(outputFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                processLine(line, writer);
            }
        }
    }

    private void processLine(String line, BufferedWriter writer) throws IOException {
        var keyValues = mapper.map(line);
        for (var keyValue : keyValues) {
            writer.write(keyValue.key().toString());
            writer.write('\t');
            writer.write(keyValue.value().toString());
            writer.newLine();
        }
    }
}
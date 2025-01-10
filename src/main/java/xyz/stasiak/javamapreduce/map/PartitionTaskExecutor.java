package xyz.stasiak.javamapreduce.map;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.files.FileManager;

class PartitionTaskExecutor {
    private static final Logger LOGGER = Logger.getLogger(PartitionTaskExecutor.class.getName());
    private final PartitionTask task;

    private record PartitionWriter(Path partitionDir, BufferedWriter writer) {
    }

    PartitionTaskExecutor(PartitionTask task) {
        this.task = task;
    }

    PartitionResult execute() {
        var currentTask = task;
        PartitionResult result = null;

        while (currentTask.canRetry()) {
            try {
                LOGGER.info("Processing file: %s".formatted(currentTask.inputFile()));
                var outputFiles = processFile(currentTask.inputFile());
                result = PartitionResult.success(outputFiles);
                break;
            } catch (Exception e) {
                LOGGER.warning("(%d) [%s] Error processing file: %s %s. Retries left: %d"
                        .formatted(currentTask.processingId(), this.getClass().getSimpleName(),
                                currentTask.inputFile(), e.getMessage(), currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = PartitionResult.failure(e.getMessage());
            }
        }

        return result;
    }

    private List<Path> processFile(Path inputFile) throws IOException {
        var partitionWriters = new HashMap<Integer, PartitionWriter>();
        var outputFiles = new ArrayList<Path>();

        try (var reader = Files.newBufferedReader(inputFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                var parts = line.split("\t");

                var key = parts[0];
                var value = parts[1];
                var partition = task.partitionFunction().apply(key);

                var writer = getOrCreateWriter(partition, partitionWriters);
                writer.writer().write(key);
                writer.writer().write('\t');
                writer.writer().write(value);
                writer.writer().newLine();
            }
        } finally {
            for (var entry : partitionWriters.entrySet()) {
                try {
                    entry.getValue().writer().close();
                    outputFiles.add(entry.getValue().partitionDir());
                } catch (IOException e) {
                    LOGGER.warning("(%d) [%s] Error closing writer for partition %d: %s"
                            .formatted(task.processingId(), this.getClass().getSimpleName(),
                                    entry.getKey(), e.getMessage()));
                }
            }
        }

        return outputFiles;
    }

    private PartitionWriter getOrCreateWriter(int partition, Map<Integer, PartitionWriter> writers) throws IOException {
        return writers.computeIfAbsent(partition, p -> {
            try {
                var partitionDir = FileManager.getPartitionDirectory(task.processingId(), partition)
                        .resolve(task.inputFile().getFileName());
                var writer = Files.newBufferedWriter(partitionDir);
                return new PartitionWriter(partitionDir, writer);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create writer for partition " + p, e);
            }
        });
    }
}
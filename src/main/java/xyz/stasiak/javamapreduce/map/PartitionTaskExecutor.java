package xyz.stasiak.javamapreduce.map;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.util.LoggingUtil;

record KeyValuePair(String key, String value) implements Comparable<KeyValuePair> {
    @Override
    public int compareTo(KeyValuePair other) {
        return this.key.compareTo(other.key);
    }
}

class PartitionTaskExecutor {
    private static final Logger LOGGER = Logger.getLogger(PartitionTaskExecutor.class.getName());
    private final PartitionTask task;

    PartitionTaskExecutor(PartitionTask task) {
        this.task = task;
    }

    PartitionResult execute() {
        var currentTask = task;
        PartitionResult result = null;

        while (currentTask.canRetry()) {
            try {
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(),
                        "Processing file: %s".formatted(currentTask.inputFile()));
                var outputFiles = processFile(currentTask.inputFile());
                result = PartitionResult.success(outputFiles);
                break;
            } catch (Exception e) {
                LoggingUtil.logWarning(LOGGER, currentTask.processingId(), getClass(),
                        "Error processing file: %s. Retries left: %d".formatted(currentTask.inputFile(),
                                currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = PartitionResult.failure(e);
            }
        }

        return result;
    }

    private List<Path> processFile(Path inputFile) throws IOException {
        var outputFiles = new ArrayList<Path>();
        var partitions = new HashMap<Integer, List<KeyValuePair>>();

        try (var reader = Files.newBufferedReader(inputFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                var parts = line.split("\t");

                var key = parts[0];
                var value = parts[1];
                var partition = task.partitionFunction().apply(key);

                partitions.computeIfAbsent(partition, _ -> new ArrayList<>())
                        .add(new KeyValuePair(key, value));
            }
        }

        for (var entry : partitions.entrySet()) {
            var partition = entry.getKey();
            var pairs = entry.getValue();
            pairs.sort(KeyValuePair::compareTo);

            var outputFile = task.outputDirectory().resolve("%d/%s".formatted(partition, inputFile.getFileName()));
            try (var writer = Files.newBufferedWriter(outputFile)) {
                for (var pair : pairs) {
                    writer.write("%s\t%s%n".formatted(pair.key(), pair.value()));
                }
            }
            outputFiles.add(outputFile);
        }

        return outputFiles;
    }
}
package xyz.stasiak.javamapreduce.reduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.logging.Logger;

class ReduceTaskExecutor {
    private static final Logger LOGGER = Logger.getLogger(ReduceTaskExecutor.class.getName());
    private final ReduceTask task;

    ReduceTaskExecutor(ReduceTask task) {
        this.task = task;
    }

    ReduceResult execute() {
        var currentTask = task;
        ReduceResult result = null;

        while (currentTask.canRetry()) {
            try {
                LOGGER.info("(%d) [%s] Processing file: %s".formatted(
                        currentTask.processingId(), this.getClass().getSimpleName(), currentTask.inputFile()));
                var outputFile = currentTask.outputDirectory().resolve(currentTask.inputFile().getFileName());
                processFile(currentTask.inputFile(), outputFile);
                result = ReduceResult.success(outputFile);
                break;
            } catch (Exception e) {
                LOGGER.warning("(%d) [%s] Error processing file: %s %s. Retries left: %d"
                        .formatted(currentTask.processingId(), this.getClass().getSimpleName(),
                                currentTask.inputFile(), e.getMessage(), currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = ReduceResult.failure(e.getMessage());
            }
        }

        return result;
    }

    private void processFile(Path inputFile, Path outputFile) throws IOException {
        try (var reader = Files.newBufferedReader(inputFile);
                var writer = Files.newBufferedWriter(outputFile)) {
            processLines(reader, writer);
        }
    }

    private void processLines(BufferedReader reader, BufferedWriter writer) throws IOException {
        String currentKey = null;
        var currentValues = new ArrayList<String>();
        String line;

        while ((line = reader.readLine()) != null) {
            var parts = line.split("\t");
            if (parts.length != 2) {
                throw new IOException("Invalid line format: " + line);
            }

            var key = parts[0];
            var value = parts[1];

            if (currentKey == null) {
                currentKey = key;
                currentValues.add(value);
            } else if (key.equals(currentKey)) {
                currentValues.add(value);
            } else {
                writeResult(writer, currentKey, currentValues);
                currentKey = key;
                currentValues.clear();
                currentValues.add(value);
            }
        }

        if (currentKey != null) {
            writeResult(writer, currentKey, currentValues);
        }
    }

    private void writeResult(BufferedWriter writer, String key, ArrayList<String> values) throws IOException {
        var result = task.reducer().reduce(key, values);
        writer.write(key);
        writer.write('\t');
        writer.write(result);
        writer.newLine();
    }
}
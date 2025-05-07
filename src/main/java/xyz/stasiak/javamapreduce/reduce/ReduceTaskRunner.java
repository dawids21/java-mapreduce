package xyz.stasiak.javamapreduce.reduce;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.processing.ProcessingCancelledException;
import xyz.stasiak.javamapreduce.util.KeyValue;
import xyz.stasiak.javamapreduce.util.LoggingUtil;

class ReduceTaskRunner {
    private static final Logger LOGGER = Logger.getLogger(ReduceTaskRunner.class.getName());
    private final ReduceTask task;

    ReduceTaskRunner(ReduceTask task) {
        this.task = task;
    }

    ReduceResult execute(boolean injectException) {
        var currentTask = task;
        ReduceResult result = null;

        while (currentTask.canRetry()) {
            try {
                currentTask.cancellationToken().throwIfCancelled(currentTask.processingId(), "Reduce task cancelled");
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(),
                        "Processing file: %s".formatted(currentTask.inputFile()));
                var outputFile = currentTask.outputDirectory().resolve(currentTask.inputFile().getFileName());
                processFile(currentTask.inputFile(), outputFile, injectException);
                result = ReduceResult.success(outputFile);
                break;
            } catch (ProcessingCancelledException e) {
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(), "Reduce task cancelled");
                result = ReduceResult.failure(e);
                break;
            } catch (Exception e) {
                LoggingUtil.logWarning(LOGGER, currentTask.processingId(), getClass(),
                        "Error processing file: %s. Retries left: %d".formatted(currentTask.inputFile(),
                                currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = ReduceResult.failure(e);
                injectException = Math.random() < 0.2;
            }
        }

        return result;
    }

    private void processFile(Path inputFile, Path outputFile, boolean injectException)
            throws IOException, ClassNotFoundException {
        try (var inputStream = Files.newInputStream(inputFile);
                var bufferedInputStream = new BufferedInputStream(inputStream, 1024 * 1024);
                var objectReader = new ObjectInputStream(bufferedInputStream);
                var writer = Files.newBufferedWriter(outputFile)) {

            processObjects(objectReader, writer, injectException);
        }
    }

    private void processObjects(ObjectInputStream reader, BufferedWriter writer, boolean injectException)
            throws IOException, ClassNotFoundException {
        String currentKey = null;
        var currentValues = new ArrayList<String>();

        try {
            while (true) {
                task.cancellationToken().throwIfCancelled(task.processingId(), "Reduce task cancelled");
                if (injectException) {
                    throw new IOException("Injected exception in reduce task");
                }

                KeyValue keyValue = (KeyValue) reader.readObject();
                String key = keyValue.key();
                String value = keyValue.value();

                if (currentKey == null) {
                    currentKey = key;
                    currentValues.add(value);
                } else if (key.equals(currentKey)) {
                    currentValues.add(value);
                } else {
                    task.cancellationToken().throwIfCancelled(task.processingId(), "Reduce task cancelled");
                    writeResult(writer, currentKey, currentValues);
                    currentKey = key;
                    currentValues.clear();
                    currentValues.add(value);
                }
            }
        } catch (EOFException e) {
            // End of file reached, process the last group
            if (currentKey != null) {
                task.cancellationToken().throwIfCancelled(task.processingId(), "Reduce task cancelled");
                writeResult(writer, currentKey, currentValues);
            }
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
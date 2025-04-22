package xyz.stasiak.javamapreduce.map;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.processing.ProcessingCancelledException;
import xyz.stasiak.javamapreduce.util.KeyValue;
import xyz.stasiak.javamapreduce.util.LoggingUtil;

class PartitionTaskRunner {
    private static final Logger LOGGER = Logger.getLogger(PartitionTaskRunner.class.getName());
    private final PartitionTask task;

    PartitionTaskRunner(PartitionTask task) {
        this.task = task;
    }

    PartitionResult execute() {
        var currentTask = task;
        PartitionResult result = null;

        while (currentTask.canRetry()) {
            try {
                currentTask.cancellationToken().throwIfCancelled(currentTask.processingId(),
                        "Partition task cancelled");
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(),
                        "Processing file: %s".formatted(currentTask.inputFile()));
                var outputFiles = processFile(currentTask.inputFile());
                result = PartitionResult.success(outputFiles);
                break;
            } catch (ProcessingCancelledException e) {
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(), "Partition task cancelled");
                result = PartitionResult.failure(e);
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

    private List<Path> processFile(Path inputFile) throws IOException, ClassNotFoundException {
        var outputFiles = new ArrayList<Path>();
        var partitions = new HashMap<Integer, List<KeyValue>>();

        try (var inputStream = Files.newInputStream(inputFile);
                var bufferedInputStream = new BufferedInputStream(inputStream, 1024 * 1024);
                var objectReader = new ObjectInputStream(bufferedInputStream)) {

            while (true) {
                try {
                    task.cancellationToken().throwIfCancelled(task.processingId(), "Partition task cancelled");

                    KeyValue keyValue = (KeyValue) objectReader.readObject();
                    var key = keyValue.key();
                    var partition = task.partitionFunction().apply(key);

                    partitions.computeIfAbsent(partition, _ -> new ArrayList<>())
                            .add(keyValue);
                } catch (EOFException e) {
                    break;
                }
            }
        }

        for (var entry : partitions.entrySet()) {
            task.cancellationToken().throwIfCancelled(task.processingId(), "Partition task cancelled");
            var partition = entry.getKey();
            var pairs = entry.getValue();
            pairs.sort(KeyValue::compareTo);

            var outputFile = task.outputDirectory().resolve("%d/%s".formatted(partition, inputFile.getFileName()));

            try (var outputStream = Files.newOutputStream(outputFile);
                    var bufferedOutputStream = new BufferedOutputStream(outputStream, 1024 * 1024);
                    var objectWriter = new ObjectOutputStream(bufferedOutputStream)) {

                for (var pair : pairs) {
                    task.cancellationToken().throwIfCancelled(task.processingId(), "Partition task cancelled");
                    objectWriter.writeObject(pair);
                }
            }
            outputFiles.add(outputFile);
        }

        return outputFiles;
    }
}
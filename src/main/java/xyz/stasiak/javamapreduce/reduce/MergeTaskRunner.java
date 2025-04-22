package xyz.stasiak.javamapreduce.reduce;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.processing.ProcessingCancelledException;
import xyz.stasiak.javamapreduce.util.KeyValue;
import xyz.stasiak.javamapreduce.util.LoggingUtil;

class MergeTaskRunner {
    private static final Logger LOGGER = Logger.getLogger(MergeTaskRunner.class.getName());
    private final MergeTask task;

    private record FileHandle(ObjectInputStream reader, KeyValue currentKeyValue, boolean isFinished) {
        FileHandle updateKeyValue(KeyValue keyValue) {
            return new FileHandle(reader, keyValue, false);
        }

        FileHandle finish() {
            return new FileHandle(reader, null, true);
        }
    }

    MergeTaskRunner(MergeTask task) {
        this.task = task;
    }

    MergeResult execute() {
        var currentTask = task;
        MergeResult result = null;

        while (currentTask.canRetry()) {
            try {
                currentTask.cancellationToken().throwIfCancelled(currentTask.processingId(), "Merge task cancelled");
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(),
                        "Merging partition: %d".formatted(currentTask.partitionId()));

                var outputFile = currentTask.outputDirectory()
                        .resolve(String.valueOf(currentTask.partitionId()));

                mergeFiles(currentTask.inputFiles(), outputFile);

                result = MergeResult.success(outputFile);
                break;
            } catch (ProcessingCancelledException e) {
                LoggingUtil.logInfo(LOGGER, currentTask.processingId(), getClass(), "Merge task cancelled");
                result = MergeResult.failure(e);
                break;
            } catch (Exception e) {
                LoggingUtil.logWarning(LOGGER, currentTask.processingId(), getClass(),
                        "Error merging partition %d: %s. Retries left: %d".formatted(currentTask.partitionId(),
                                e.getMessage(), currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = MergeResult.failure(e);
            }
        }

        return result;
    }

    private void mergeFiles(List<Path> inputFiles, Path outputFile) throws IOException, ClassNotFoundException {
        var fileHandles = openInputFiles(inputFiles);
        try (var outputStream = Files.newOutputStream(outputFile);
                var bufferedOutputStream = new BufferedOutputStream(outputStream, 1024 * 1024);
                var objectWriter = new ObjectOutputStream(bufferedOutputStream)) {

            mergeFileHandles(fileHandles, objectWriter);
        } finally {
            closeFileHandles(fileHandles);
        }
    }

    private List<FileHandle> openInputFiles(List<Path> inputFiles) throws IOException, ClassNotFoundException {
        var handles = new ArrayList<FileHandle>();
        for (var file : inputFiles) {
            task.cancellationToken().throwIfCancelled(task.processingId(), "Merge task cancelled");
            var inputStream = Files.newInputStream(file);
            var bufferedInputStream = new BufferedInputStream(inputStream, 1024 * 1024);
            var reader = new ObjectInputStream(bufferedInputStream);

            KeyValue keyValue = null;
            try {
                keyValue = (KeyValue) reader.readObject();
                handles.add(new FileHandle(reader, keyValue, false));
            } catch (EOFException | ClassNotFoundException e) {
                handles.add(new FileHandle(reader, null, true));
                bufferedInputStream.close();
            }
        }
        return handles;
    }

    private void mergeFileHandles(List<FileHandle> fileHandles, ObjectOutputStream writer)
            throws IOException, ClassNotFoundException {
        int finishedFiles = 0;
        while (finishedFiles < fileHandles.size()) {
            task.cancellationToken().throwIfCancelled(task.processingId(), "Merge task cancelled");
            var minHandle = findMinimumKeyValue(fileHandles);

            if (minHandle.currentKeyValue != null) {
                writer.writeObject(minHandle.currentKeyValue);
            }

            var reader = minHandle.reader;
            KeyValue nextKeyValue = null;
            try {
                nextKeyValue = (KeyValue) reader.readObject();
            } catch (EOFException e) {
            }

            var handleIndex = fileHandles.indexOf(minHandle);
            if (nextKeyValue != null) {
                fileHandles.set(handleIndex, minHandle.updateKeyValue(nextKeyValue));
            } else {
                finishedFiles++;
                fileHandles.set(handleIndex, minHandle.finish());
            }
        }
    }

    private FileHandle findMinimumKeyValue(List<FileHandle> fileHandles) {
        FileHandle minHandle = null;
        KeyValue minKeyValue = null;

        for (var handle : fileHandles) {
            if (!handle.isFinished) {
                minHandle = handle;
                minKeyValue = handle.currentKeyValue;
                break;
            }
        }

        if (minHandle == null) {
            return fileHandles.get(0);
        }

        for (var handle : fileHandles) {
            if (!handle.isFinished &&
                    (minKeyValue == null ||
                            handle.currentKeyValue.compareTo(minKeyValue) < 0)) {
                minHandle = handle;
                minKeyValue = handle.currentKeyValue;
            }
        }

        return minHandle;
    }

    private void closeFileHandles(List<FileHandle> fileHandles) throws IOException {
        for (var handle : fileHandles) {
            handle.reader.close();
        }
    }
}
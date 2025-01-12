package xyz.stasiak.javamapreduce.reduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

class MergeTaskExecutor {
    private static final Logger LOGGER = Logger.getLogger(MergeTaskExecutor.class.getName());
    private final MergeTask task;

    private record FileHandle(BufferedReader reader, String currentLine, boolean isFinished) {
        FileHandle updateLine(String line) {
            return new FileHandle(reader, line, false);
        }

        FileHandle finish() {
            return new FileHandle(reader, null, true);
        }
    }

    MergeTaskExecutor(MergeTask task) {
        this.task = task;
    }

    MergeResult execute() {
        var currentTask = task;
        MergeResult result = null;

        while (currentTask.canRetry()) {
            try {
                LOGGER.info("(%d) [%s] Merging partition: %d"
                        .formatted(currentTask.processingId(), this.getClass().getSimpleName(),
                                currentTask.partitionId()));

                var outputFile = currentTask.outputDirectory()
                        .resolve(String.valueOf(currentTask.partitionId()));

                mergeFiles(currentTask.inputFiles(), outputFile);

                result = MergeResult.success(outputFile);
                break;
            } catch (Exception e) {
                LOGGER.warning("(%d) [%s] Error merging partition %d: %s. Retries left: %d"
                        .formatted(currentTask.processingId(), this.getClass().getSimpleName(),
                                currentTask.partitionId(), e.getMessage(), currentTask.maxRetries() - 1));
                currentTask = currentTask.withIncrementedRetries();
                result = MergeResult.failure(e.getMessage());
            }
        }

        return result;
    }

    private void mergeFiles(List<Path> inputFiles, Path outputFile) throws IOException {
        var fileHandles = openInputFiles(inputFiles);
        try (var writer = Files.newBufferedWriter(outputFile)) {
            mergeFileHandles(fileHandles, writer);
        } finally {
            closeFileHandles(fileHandles);
        }
    }

    private List<FileHandle> openInputFiles(List<Path> inputFiles) throws IOException {
        var handles = new ArrayList<FileHandle>();
        for (var file : inputFiles) {
            var reader = Files.newBufferedReader(file);
            var line = reader.readLine();
            if (line != null) {
                handles.add(new FileHandle(reader, line, false));
            } else {
                handles.add(new FileHandle(reader, null, true));
            }
        }
        return handles;
    }

    private void mergeFileHandles(List<FileHandle> fileHandles, BufferedWriter writer) throws IOException {
        int finishedFiles = 0;
        while (finishedFiles < fileHandles.size()) {
            var minHandle = findMinimumLine(fileHandles);
            writer.write(minHandle.currentLine);
            writer.newLine();

            var reader = minHandle.reader;
            var nextLine = reader.readLine();
            var handleIndex = fileHandles.indexOf(minHandle);
            if (nextLine != null) {
                fileHandles.set(handleIndex, minHandle.updateLine(nextLine));
            } else {
                finishedFiles++;
                fileHandles.set(handleIndex, minHandle.finish());
            }
        }
    }

    private FileHandle findMinimumLine(List<FileHandle> fileHandles) {
        var minHandle = fileHandles.get(0);
        var minLine = minHandle.currentLine;

        for (var handle : fileHandles) {
            if (minHandle.isFinished || (!handle.isFinished() && handle.currentLine.compareTo(minLine) < 0)) {
                minHandle = handle;
                minLine = handle.currentLine;
            }
        }

        return minHandle;
    }

    private void closeFileHandles(List<FileHandle> fileHandles) {
        for (var handle : fileHandles) {
            try {
                handle.reader.close();
            } catch (IOException e) {
                LOGGER.warning("(%d) [%s] Error closing file handle: %s"
                        .formatted(task.processingId(), this.getClass().getSimpleName(), e.getMessage()));
            }
        }
    }
}
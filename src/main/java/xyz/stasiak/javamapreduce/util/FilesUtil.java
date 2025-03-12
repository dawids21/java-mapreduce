package xyz.stasiak.javamapreduce.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class FilesUtil {
    private static final Logger LOGGER = Logger.getLogger(FilesUtil.class.getName());
    private static final String DEFAULT_NODE_DIRECTORY = SystemProperties.getNodeDirectory();
    private static final String DEFAULT_PUBLIC_DIRECTORY = SystemProperties.getPublicDirectory();

    public static Path getBaseNodeDirectory(int processingId) {
        return Path.of(DEFAULT_NODE_DIRECTORY, String.valueOf(processingId));
    }

    public static Path getBasePublicDirectory(int processingId) {
        return Path.of(DEFAULT_PUBLIC_DIRECTORY, String.valueOf(processingId));
    }

    public static Path getMapFilesDirectory(int processingId) {
        return getBaseNodeDirectory(processingId).resolve("map");
    }

    public static Path getPartitionFilesDirectory(int processingId) {
        return getBasePublicDirectory(processingId).resolve("partition");
    }

    public static Path getPartitionDirectory(int processingId, int partitionId) {
        return getPartitionFilesDirectory(processingId).resolve(String.valueOf(partitionId));
    }

    public static List<Integer> getPartitions(int processingId) throws IOException {
        var directory = getPartitionFilesDirectory(processingId).toFile();
        var partitionDirectories = directory.list();
        if (partitionDirectories == null) {
            LoggingUtil.logWarning(LOGGER, processingId, FilesUtil.class, "Failed to list partition directories");
            return List.of();
        }
        var partitions = Arrays.stream(partitionDirectories)
                .map(dirName -> Integer.parseInt(dirName))
                .toList();
        return partitions;
    }

    public static Path getMergeFilesDirectory(int processingId) {
        return getBaseNodeDirectory(processingId).resolve("merge");
    }

    public static void createNodeDirectories(int processingId) throws IOException {
        Files.createDirectories(getMapFilesDirectory(processingId));
        Files.createDirectories(getMergeFilesDirectory(processingId));
    }

    public static void createPublicDirectories(int processingId, String outputDirectory) throws IOException {
        Files.createDirectories(getPartitionFilesDirectory(processingId));
        if (Files.exists(Path.of(outputDirectory))) {
            deleteDirectory(Path.of(outputDirectory));
        }
        Files.createDirectories(Path.of(outputDirectory));
    }

    public static void createPartitionDirectories(int processingId, int totalPartitions) throws IOException {
        for (int i = 0; i < totalPartitions; i++) {
            Files.createDirectories(getPartitionDirectory(processingId, i));
        }
    }

    public static void removeEmptyPartitionDirectories(int processingId) throws IOException {
        LoggingUtil.logInfo(LOGGER, processingId, FilesUtil.class, "Removing empty partition directories");

        var partitionDirectory = getPartitionFilesDirectory(processingId).toFile();
        var partitionDirectories = partitionDirectory.list();
        if (partitionDirectories == null) {
            return;
        }
        for (String partitionPathStr : partitionDirectories) {
            var partitionPath = Path.of(partitionDirectory.getAbsolutePath(), partitionPathStr);
            var partition = partitionPath.toFile();
            var partitionFiles = partition.list();
            if (partitionFiles == null) {
                continue;
            }
            if (partitionFiles.length == 0) {
                try {
                    deleteDirectory(partitionPath);
                } catch (IOException e) {
                    LoggingUtil.logWarning(LOGGER, processingId, FilesUtil.class,
                            "Failed to check or delete directory: " + partitionPath, e);
                }
            }
        }
    }

    public static void removeNodeDirectories(int processingId) throws IOException {
        if (Files.exists(getBaseNodeDirectory(processingId))) {
            deleteDirectory(getBaseNodeDirectory(processingId));
        }
    }

    public static void removePublicDirectories(int processingId) throws IOException {
        if (Files.exists(getBasePublicDirectory(processingId))) {
            deleteDirectory(getBasePublicDirectory(processingId));
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
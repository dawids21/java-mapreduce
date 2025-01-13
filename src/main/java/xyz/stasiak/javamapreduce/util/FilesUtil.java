package xyz.stasiak.javamapreduce.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.Application;

public class FilesUtil {
    private static final Logger LOGGER = Logger.getLogger(FilesUtil.class.getName());
    private static final String PUBLIC_DIRECTORY_PROPERTY = "public.directory";
    private static final String DEFAULT_NODE_DIRECTORY = "/tmp/java-mapreduce";

    public static Path getBaseNodeDirectory(int processingId) {
        return Path.of(DEFAULT_NODE_DIRECTORY, String.valueOf(processingId));
    }

    public static Path getBasePublicDirectory(int processingId) {
        var publicDirectory = Application.getProperties().getProperty(PUBLIC_DIRECTORY_PROPERTY);
        if (publicDirectory == null) {
            throw new IllegalStateException("Public directory property not set in application.properties");
        }
        return Path.of(publicDirectory, String.valueOf(processingId));
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
        try (var paths = Files.list(getPartitionFilesDirectory(processingId))) {
            return paths.map(path -> Integer.parseInt(path.getFileName().toString()))
                    .toList();
        }
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
        try (var paths = Files.list(getPartitionFilesDirectory(processingId))) {
            paths.forEach(path -> {
                try {
                    if (Files.list(path).findFirst().isEmpty()) {
                        deleteDirectory(path);
                    }
                } catch (IOException e) {
                    LOGGER.warning("Failed to check or delete directory: " + path);
                }
            });
        }
    }

    public static void removeNodeDirectories(int processingId) throws IOException {
        deleteDirectory(getBaseNodeDirectory(processingId));
    }

    public static void removePublicDirectories(int processingId) throws IOException {
        deleteDirectory(getBasePublicDirectory(processingId));
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
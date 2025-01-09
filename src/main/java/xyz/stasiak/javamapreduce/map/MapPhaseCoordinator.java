package xyz.stasiak.javamapreduce.map;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.files.FileManager;
public class MapPhaseCoordinator {
    private static final Logger LOGGER = Logger.getLogger(MapPhaseCoordinator.class.getName());
    private final int processingId;
    private final String mapperClassName;
    private final List<Path> files;
    private final ExecutorService executor;

    public MapPhaseCoordinator(int processingId, String mapperClassName, List<Path> files) {
        this.processingId = processingId;
        this.mapperClassName = mapperClassName;
        this.files = files;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public List<MapResult> execute() {
        LOGGER.info("(%d) [%s] Starting map phase with %d files".formatted(processingId,
                MapPhaseCoordinator.class.getSimpleName(), files.size()));
        var mapper = MapperFactory.createMapper(mapperClassName);
        var futures = new ArrayList<Future<MapResult>>();

        for (var file : files) {
            var task = new MapTask(processingId, file, FileManager.getMapFilesDirectory(processingId), mapperClassName,
                    3, null);
            var executor = new MapTaskExecutor<>(task, mapper);
            futures.add(this.executor.submit(executor::execute));
        }

        var results = new ArrayList<MapResult>();
        var failedFiles = new ArrayList<Path>();

        for (var i = 0; i < futures.size(); i++) {
            try {
                var result = futures.get(i).get();
                results.add(result);
                if (!result.success()) {
                    failedFiles.add(files.get(i));
                }
            } catch (Exception e) {
                LOGGER.severe("(%d) [%s] Error executing map task: %s".formatted(processingId,
                        MapPhaseCoordinator.class.getSimpleName(), e.getMessage()));
                failedFiles.add(files.get(i));
                executor.close();
                throw new RuntimeException("(%d) [%s] Map phase failed: %s".formatted(processingId,
                        MapPhaseCoordinator.class.getSimpleName(), e.getMessage()), e);
            }
        }

        executor.close();

        if (!failedFiles.isEmpty()) {
            var message = "(%d) [%s] Map phase failed with %d failed files".formatted(processingId,
                    MapPhaseCoordinator.class.getSimpleName(), failedFiles.size());
            LOGGER.severe(message);
            throw new RuntimeException(message);
        }

        LOGGER.info("(%d) [%s] Map phase completed successfully".formatted(processingId,
                MapPhaseCoordinator.class.getSimpleName()));

        return results;
    }
}

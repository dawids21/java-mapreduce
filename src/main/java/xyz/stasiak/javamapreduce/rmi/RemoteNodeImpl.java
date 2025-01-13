package xyz.stasiak.javamapreduce.rmi;

import java.io.IOException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.Application;
import xyz.stasiak.javamapreduce.map.MapPhaseCoordinator;
import xyz.stasiak.javamapreduce.reduce.ReducePhaseCoordinator;
import xyz.stasiak.javamapreduce.util.FilesUtil;

public class RemoteNodeImpl extends UnicastRemoteObject implements RemoteNode {

    private static final Logger LOGGER = Logger.getLogger(RemoteNodeImpl.class.getSimpleName());

    private final Map<Integer, ProcessingState> processingStates;
    private final Map<Integer, ProcessingInfo> processingInfos;
    private final WorkDistributor workDistributor = new WorkDistributor();

    public RemoteNodeImpl() throws RemoteException {
        super();
        this.processingStates = new ConcurrentHashMap<>();
        this.processingInfos = new ConcurrentHashMap<>();
    }

    public void startProcessing(int processingId, ProcessingParameters parameters) throws RemoteException {
        var activeNodes = workDistributor.getActiveNodes(processingId);

        var totalPartitions = workDistributor.calculateTotalPartitions(processingId, activeNodes);
        Function<String, Integer> partitionFunction = workDistributor.createPartitionFunction(totalPartitions);
        try {
            FilesUtil.createPartitionDirectories(processingId, totalPartitions);
        } catch (IOException e) {
            LOGGER.severe("(%d) [%s] Failed to create partition directories: %s".formatted(processingId,
                    this.getClass().getSimpleName(), e.getMessage()));
            throw new RuntimeException("Failed to create partition directories", e);
        }
        var fileAssignments = workDistributor.distributeFiles(processingId, activeNodes, parameters.inputDirectory());
        var totalFiles = fileAssignments.values().stream()
                .mapToInt(List::size)
                .sum();
        var processingState = ProcessingState
                .create(processingId, activeNodes, totalFiles, totalPartitions)
                .updateFileAssignments(fileAssignments);
        processingStates.put(processingId, processingState);

        var masterNode = Application.getProperty("node.address");
        activeNodes.forEach(node -> CompletableFuture.runAsync(() -> {
            try {
                var files = processingState.fileAssignments().get(node);
                if (node.equals(masterNode)) {
                    startNodeProcessing(processingId, parameters, partitionFunction, masterNode);
                    processingStates.compute(processingId, (_, state) -> state.updateStatus(ProcessingStatus.MAPPING));
                    startMapPhase(processingId, files);
                } else {
                    var remoteNode = RmiUtil.getRemoteNode(node);
                    remoteNode.startNodeProcessing(processingId, parameters, partitionFunction, masterNode);
                    remoteNode.startMapPhase(processingId, files);
                }
            } catch (Exception e) {
                LOGGER.severe("(%d) [%s] Failed to map phase on %s: %s".formatted(
                        processingId, this.getClass().getSimpleName(), node, e.getMessage()));
                throw new CompletionException(e);
                // TODO handle failure, node may be dead
            }
        }));
    }

    @Override
    public void startNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction, String masterNode)
            throws RemoteException {
        LOGGER.info("(%d) [%s] Starting node processing".formatted(processingId,
                this.getClass().getSimpleName()));
        try {
            FilesUtil.createNodeDirectories(processingId);
        } catch (IOException e) {
            LOGGER.severe("(%d) [%s] Failed to create directories: %s".formatted(processingId,
                    this.getClass().getSimpleName(), e.getMessage()));
            throw new RemoteException("Failed to create directories", e);
        }
        var processingInfo = new ProcessingInfo(processingId, parameters, masterNode, partitionFunction);
        processingInfos.put(processingId, processingInfo);
    }

    @Override
    public void startMapPhase(int processingId, List<String> files) throws RemoteException {
        LOGGER.info("(%d) [%s] Starting map phase".formatted(processingId, this.getClass().getSimpleName()));
        var processingInfo = processingInfos.get(processingId);
        var paths = files.stream().map(Path::of).toList();

        try {
            var coordinator = new MapPhaseCoordinator(processingId, processingInfo.parameters().mapperClassName(),
                    Path.of(processingInfo.parameters().inputDirectory()), paths, processingInfo.partitionFunction());
            var result = coordinator.execute();
            var nodeAddress = Application.getProperty("node.address");
            var masterNode = processingInfo.masterNode();
            var remoteNode = RmiUtil.getRemoteNode(masterNode);
            remoteNode.finishMapPhase(processingId, nodeAddress, result.processedFiles());
        } catch (Exception e) {
            LOGGER.severe("(%d) [%s] Map phase failed: %s".formatted(processingId,
                    this.getClass().getSimpleName(), e.getMessage()));
            throw new RemoteException("Map phase failed", e);
        }
    }

    @Override
    public void finishMapPhase(int processingId, String node, int processedFiles) throws RemoteException {
        var state = processingStates.compute(processingId,
                (_, processingState) -> processingState.addProcessedFiles(processedFiles));

        if (state.isMapPhaseCompleted()) {
            LOGGER.info("(%d) [%s] Map phase completed on all nodes".formatted(
                    processingId, this.getClass().getSimpleName()));
            try {
                FilesUtil.removeEmptyPartitionDirectories(processingId);
            } catch (IOException e) {
                LOGGER.severe("(%d) [%s] Failed to remove empty partition directories: %s".formatted(
                        processingId, this.getClass().getSimpleName(), e.getMessage()));
                throw new RuntimeException("Failed to remove empty partition directories", e);
            }

            var updatedState = processingStates.compute(processingId, (_, processingState) -> {
                var activeNodes = processingState.activeNodes();
                try {
                    var partitionAssignments = workDistributor.distributePartitions(
                            processingId, activeNodes);
                    return processingState.updatePartitionAssignments(partitionAssignments);
                } catch (IOException e) {
                    LOGGER.severe("(%d) [%s] Failed to distribute partitions: %s".formatted(
                            processingId, this.getClass().getSimpleName(), e.getMessage()));
                    throw new RuntimeException("Failed to distribute partitions", e);
                }
            });

            updatedState.activeNodes().forEach(activeNode -> CompletableFuture.runAsync(() -> {
                try {
                    var partitions = updatedState.partitionAssignments().get(activeNode);
                    if (activeNode.equals(Application.getProperty("node.address"))) {
                        processingStates.compute(processingId,
                                (_, processingState) -> processingState.updateStatus(ProcessingStatus.REDUCING));
                        startReducePhase(processingId, partitions);
                    } else {
                        var remoteNode = RmiUtil.getRemoteNode(activeNode);
                        remoteNode.startReducePhase(processingId, partitions);
                    }
                } catch (Exception e) {
                    LOGGER.severe("(%d) [%s] Failed to start reduce phase on %s: %s".formatted(
                            processingId, this.getClass().getSimpleName(), activeNode, e.getMessage()));
                    throw new CompletionException(e);
                    // TODO handle failure, node may be dead
                }
            }));
        }
    }

    @Override
    public void startReducePhase(int processingId, List<Integer> partitions)
            throws RemoteException {
        LOGGER.info("(%d) [%s] Starting reduce phase".formatted(processingId, this.getClass().getSimpleName()));

        var processingInfo = processingInfos.get(processingId);
        if (processingInfo == null) {
            throw new IllegalStateException("Processing info %d not found".formatted(processingId));
        }

        if (partitions == null || partitions.isEmpty()) {
            LOGGER.info("(%d) [%s] No partitions assigned to this node".formatted(
                    processingId, this.getClass().getSimpleName()));
            return;
        }

        try {
            var outputDirectory = Path.of(processingInfo.parameters().outputDirectory());

            var coordinator = new ReducePhaseCoordinator(
                    processingId,
                    processingInfo.parameters().reducerClassName(),
                    partitions,
                    outputDirectory);
            var result = coordinator.execute();

            var nodeAddress = Application.getProperty("node.address");
            var masterNode = processingInfo.masterNode();
            var remoteNode = RmiUtil.getRemoteNode(masterNode);
            remoteNode.finishReducePhase(processingId, nodeAddress, result.processedPartitions());
        } catch (Exception e) {
            LOGGER.severe("(%d) [%s] Reduce phase failed: %s".formatted(
                    processingId, this.getClass().getSimpleName(), e.getMessage()));
            throw new RemoteException("Reduce phase failed", e);
        }
    }

    @Override
    public void finishReducePhase(int processingId, String node, int processedPartitions)
            throws RemoteException {
        var state = processingStates.compute(processingId,
                (_, processingState) -> processingState.addProcessedPartitions(processedPartitions));

        if (state.isReducePhaseCompleted()) {
            LOGGER.info("(%d) [%s] Reduce phase completed on all nodes".formatted(
                    processingId, this.getClass().getSimpleName()));
            var updatedState = processingStates.compute(processingId,
                    (_, processingState) -> processingState.updateStatus(ProcessingStatus.FINISHED));
            updatedState.activeNodes().forEach(activeNode -> CompletableFuture.runAsync(() -> {
                try {
                    if (activeNode.equals(Application.getProperty("node.address"))) {
                        processingStates.compute(processingId,
                                (_, processingState) -> processingState.updateStatus(ProcessingStatus.FINISHED));
                        finishProcessing(processingId);
                    } else {
                        var remoteNode = RmiUtil.getRemoteNode(activeNode);
                        remoteNode.finishProcessing(processingId);
                    }
                } catch (Exception e) {
                    LOGGER.severe("(%d) [%s] Failed to finish processing on %s: %s".formatted(
                            processingId, this.getClass().getSimpleName(), activeNode, e.getMessage()));
                    throw new CompletionException(e);
                    // TODO handle failure, node may be dead
                }
            }));
        }
    }

    @Override
    public void notifyNodeFailure(int processingId, String failedNode, String masterNode) throws RemoteException {
        // TODO redistribute and update state
    }

    @Override
    public void finishProcessing(int processingId) throws RemoteException {
        var processingInfo = processingInfos.remove(processingId);
        var nodeAddress = Application.getProperty("node.address");
        if (nodeAddress.equals(processingInfo.masterNode())) {
            try {
                FilesUtil.removePublicDirectories(processingId);
            } catch (IOException e) {
                LOGGER.severe("(%d) [%s] Failed to remove public directories: %s".formatted(
                        processingId, this.getClass().getSimpleName(), e.getMessage()));
                throw new RuntimeException("Failed to remove public directories", e);
            }
        }
        try {
            FilesUtil.removeNodeDirectories(processingId);
        } catch (IOException e) {
            LOGGER.severe("(%d) [%s] Failed to remove node directories: %s".formatted(
                    processingId, this.getClass().getSimpleName(), e.getMessage()));
            throw new RuntimeException("Failed to remove node directories", e);
        }
        LOGGER.info("(%d) [%s] Processing completed successfully".formatted(
                processingId, this.getClass().getSimpleName()));
    }

    @Override
    public int getProcessingPower() throws RemoteException {
        return Runtime.getRuntime().availableProcessors();
    }

    public ProcessingStatus getProcessingStatus(int processingId) {
        return processingStates.get(processingId).status();
    }

    @Override
    public void isAlive() throws RemoteException {
    }
}

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
import xyz.stasiak.javamapreduce.files.FileManager;
import xyz.stasiak.javamapreduce.map.MapPhaseCoordinator;

public class RemoteNodeImpl extends UnicastRemoteObject implements RemoteNode {

    private static final Logger LOGGER = Logger.getLogger(RemoteNodeImpl.class.getSimpleName());

    private final Map<Integer, ProcessingState> processingStates;
    private final WorkDistributor workDistributor = new WorkDistributor();

    public RemoteNodeImpl() throws RemoteException {
        super();
        this.processingStates = new ConcurrentHashMap<>();
    }

    public void startProcessing(int processingId, ProcessingParameters parameters) throws RemoteException {
        var activeNodes = workDistributor.getActiveNodes(processingId);
        var totalPartitions = workDistributor.calculateTotalPartitions(processingId, activeNodes);
        Function<String, Integer> partitionFunction = workDistributor.createPartitionFunction(totalPartitions);
        var fileAssignments = workDistributor.distributeFiles(processingId, activeNodes, parameters.inputDirectory());
        var nodeAddress = Application.getProperty("node.address");

        activeNodes.forEach(node -> CompletableFuture.runAsync(() -> {
            try {
                if (node.equals(nodeAddress)) {
                    startNodeProcessing(processingId, parameters, partitionFunction, activeNodes, nodeAddress);
                    startMapPhase(processingId, fileAssignments);
                } else {
                    var remoteNode = RmiUtil.getRemoteNode(node);
                    remoteNode.startNodeProcessing(processingId, parameters, partitionFunction, activeNodes,
                            nodeAddress);
                    remoteNode.startMapPhase(processingId, fileAssignments);
                }
            } catch (Exception e) {
                LOGGER.severe("(%d) [%s] Failed to map phase on %s: %s".formatted(
                        processingId, this.getClass().getSimpleName(), node, e.getMessage()));
                throw new CompletionException(e);
                // TODO handle failure
            }
        }));
    }

    @Override
    public void startNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction, List<String> activeNodes, String nodeAddress)
            throws RemoteException {
        LOGGER.info("(%d) [%s] Starting node processing".formatted(processingId,
                this.getClass().getSimpleName()));
        try {
            FileManager.createNodeDirectories(processingId);
        } catch (IOException e) {
            LOGGER.severe("(%d) [%s] Failed to create directories: %s".formatted(processingId,
                    this.getClass().getSimpleName(), e.getMessage()));
            throw new RemoteException("Failed to create directories", e);
        }
        var state = ProcessingState.create(processingId, parameters, nodeAddress, activeNodes, partitionFunction);
        processingStates.put(processingId, state);
    }

    @Override
    public void startMapPhase(int processingId, Map<String, List<String>> fileAssignments) throws RemoteException {
        LOGGER.info("(%d) [%s] Starting map phase".formatted(processingId, this.getClass().getSimpleName()));
        var state = processingStates.get(processingId);
        state.updateStatus(ProcessingStatus.MAPPING);

        state.updateFileAssignments(fileAssignments);
        var files = fileAssignments.get(Application.getProperty("node.address"));
        var filePaths = files.stream().map(Path::of).toList();

        try {
            var coordinator = new MapPhaseCoordinator(processingId, state.parameters().mapperClassName(), filePaths);
            var results = coordinator.execute();
            // TODO call finishMapPhase on master
        } catch (Exception e) {
            LOGGER.severe("(%d) [%s] Map phase failed: %s".formatted(processingId,
                    this.getClass().getSimpleName(), e.getMessage()));
            throw new RemoteException("Map phase failed", e);
        }
    }

    @Override
    public void finishMapPhase(int processingId, String node, String masterNode) throws RemoteException {
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.mapCompletedNodes().add(node);
        // TODO start reduce phase
    }

    @Override
    public void startReducePhase(int processingId, Map<String, List<Integer>> partitionAssignments)
            throws RemoteException {
        // TODO set status in Application
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.updatePartitionAssignments(partitionAssignments);
        // TODO start reduce phase
    }

    @Override
    public void finishReducePhase(int processingId, String node, String masterNode) throws RemoteException {
        // TODO call master
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.reduceCompletedNodes().add(node);
        // TODO finish processing
    }

    @Override
    public void notifyNodeFailure(int processingId, String failedNode, String masterNode) throws RemoteException {
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.activeNodes().remove(failedNode);
        state.mapCompletedNodes().remove(failedNode);
        state.reduceCompletedNodes().remove(failedNode);
        state.nodeAssignments().remove(failedNode);
        // TODO redistribute
    }

    @Override
    public void finishProcessing(int processingId) throws RemoteException {
        processingStates.remove(processingId);
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

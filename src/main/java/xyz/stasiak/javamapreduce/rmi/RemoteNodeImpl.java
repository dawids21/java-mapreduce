package xyz.stasiak.javamapreduce.rmi;

import java.io.IOException;
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
import xyz.stasiak.javamapreduce.util.LoggingUtil;

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

    public void startProcessing(int processingId, ProcessingParameters parameters) {
        CompletableFuture.runAsync(() -> {
            var activeNodes = workDistributor.getActiveNodes(processingId);
            var totalPartitions = workDistributor.calculateTotalPartitions(processingId, activeNodes);
            Function<String, Integer> partitionFunction = workDistributor.createPartitionFunction(totalPartitions);
            try {
                FilesUtil.createPublicDirectories(processingId, parameters.outputDirectory());
                FilesUtil.createPartitionDirectories(processingId, totalPartitions);
            } catch (IOException e) {
                LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        "Failed to create public and partition directories", e);
                throw new ProcessingException("Failed to create public and partition directories", e);
            }
            var fileAssignments = workDistributor.distributeFiles(processingId, activeNodes,
                    parameters.inputDirectory());
            var processingState = ProcessingState
                    .create(processingId, activeNodes)
                    .setFileAssignments(fileAssignments);
            processingStates.put(processingId, processingState);

            var masterNode = Application.getNodeAddress();

            activeNodes.forEach(node -> {
                var files = processingState.fileAssignments().get(node);
                if (node.equals(masterNode)) {
                    startNodeProcessing(processingId, parameters, partitionFunction, masterNode);
                    processingStates.compute(processingId,
                            (_, state) -> state.updateStatus(ProcessingStatus.MAPPING));
                    startMapPhase(processingId, files);
                } else {
                    try {
                        var remoteNode = RmiUtil.getRemoteNode(node);
                        remoteNode.remoteStartNodeProcessing(processingId, parameters, partitionFunction, masterNode);
                        remoteNode.remoteStartMapPhase(processingId, files);
                    } catch (RemoteException | RemoteNodeUnavailableException | ProcessingException e) {
                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                "Failed to start map phase on %s".formatted(node), e);
                        handleNodeFailure(processingId, node);
                    }
                }
            });
        })
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to start processing", e);
                    failProcessing(processingId);
                    return null;
                });
    }

    @Override
    public void remoteStartNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction, String masterNode) throws RemoteException {
        startNodeProcessing(processingId, parameters, partitionFunction, masterNode);
    }

    private void startNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction, String masterNode) {
        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "Starting node processing");
        try {
            FilesUtil.createNodeDirectories(processingId);
        } catch (IOException e) {
            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                    "Failed to create directories", e);
            throw new ProcessingException("Failed to create directories", e);
        }
        var processingInfo = new ProcessingInfo(processingId, parameters, masterNode, partitionFunction);
        processingInfos.put(processingId, processingInfo);
    }

    @Override
    public void remoteStartMapPhase(int processingId, List<String> files) throws RemoteException {
        startMapPhase(processingId, files);
    }

    private void startMapPhase(int processingId, List<String> files) {
        CompletableFuture.runAsync(() -> {
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Starting map phase");
            var processingInfo = processingInfos.get(processingId);
            var coordinator = new MapPhaseCoordinator(processingId, processingInfo.parameters().mapperClassName(),
                    processingInfo.parameters().inputDirectory(), files, processingInfo.partitionFunction());
            var result = coordinator.execute();
            var nodeAddress = Application.getNodeAddress();
            var masterNode = processingInfo.masterNode();
            if (masterNode.equals(nodeAddress)) {
                finishMapPhase(processingId, nodeAddress, result.processedFiles());
            } else {
                try {
                    var remoteNode = RmiUtil.getRemoteNode(masterNode);
                    remoteNode.remoteFinishMapPhase(processingId, nodeAddress, result.processedFiles());
                } catch (RemoteException | RemoteNodeUnavailableException e) {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish map phase on %s, master node unavailable".formatted(masterNode), e);
                    cleanup(processingId);
                }
            }
        })
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Map phase failed", e);
                    var processingInfo = processingInfos.get(processingId);
                    var masterNode = processingInfo.masterNode();
                    var nodeAddress = Application.getNodeAddress();
                    if (masterNode.equals(nodeAddress)) {
                        // TODO cancel processing on other nodes
                    } else {
                        cleanup(processingId);
                        try {
                            var remoteNode = RmiUtil.getRemoteNode(masterNode);
                            remoteNode.remoteHandleNodeFailure(processingId, nodeAddress);
                        } catch (RemoteException | RemoteNodeUnavailableException e2) {
                            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                    "Map phase failed on %s and master node unavailable".formatted(masterNode), e2);
                        }
                    }
                    return null;
                });
    }

    @Override
    public void remoteFinishMapPhase(int processingId, String node, List<String> processedFiles)
            throws RemoteException {
        finishMapPhase(processingId, node, processedFiles);
    }

    private void finishMapPhase(int processingId, String node, List<String> processedFiles) {
        CompletableFuture.runAsync(() -> {
            processingStates.compute(processingId,
                    (_, processingState) -> {
                        var newState = processingState.addProcessedFiles(processedFiles);
                        if (newState.isMapPhaseCompleted()) {
                            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                                    "Map phase completed on all nodes");
                            try {
                                FilesUtil.removeEmptyPartitionDirectories(processingId);
                            } catch (IOException e) {
                                LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                        "Failed to remove empty partition directories", e);
                                throw new ProcessingException("Failed to remove empty partition directories", e);
                            }

                            var activeNodes = processingState.activeNodes();
                            var partitionAssignments = workDistributor.distributePartitions(
                                    processingId, activeNodes);
                            newState = newState.setPartitionAssignments(partitionAssignments)
                                    .updateStatus(ProcessingStatus.REDUCING);
                            newState.activeNodes().forEach(activeNode -> {
                                var partitions = partitionAssignments.get(activeNode);
                                if (activeNode.equals(Application.getNodeAddress())) {
                                    startReducePhase(processingId, partitions);
                                } else {
                                    try {
                                        var remoteNode = RmiUtil.getRemoteNode(activeNode);
                                        remoteNode.remoteStartReducePhase(processingId, partitions);
                                    } catch (RemoteException | RemoteNodeUnavailableException e) {
                                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                                "Failed to start reduce phase on %s".formatted(node), e);
                                        handleNodeFailure(processingId, activeNode);
                                    }
                                }
                            });
                        }
                        return newState;
                    });
        })
                .exceptionally(e -> {
                    // e has type CompletionException
                    // TODO handle failure in starting reduce phase on master
                    throw new CompletionException(e);
                });
    }

    @Override
    public void remoteStartReducePhase(int processingId, List<Integer> partitions)
            throws RemoteException {
        startReducePhase(processingId, partitions);
    }

    private void startReducePhase(int processingId, List<Integer> partitions) {
        CompletableFuture.runAsync(() -> {
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Starting reduce phase");
            var processingInfo = processingInfos.get(processingId);
            var coordinator = new ReducePhaseCoordinator(
                    processingId,
                    processingInfo.parameters().reducerClassName(),
                    partitions,
                    processingInfo.parameters().outputDirectory());
            var result = coordinator.execute();
            var nodeAddress = Application.getNodeAddress();
            var masterNode = processingInfo.masterNode();
            if (masterNode.equals(nodeAddress)) {
                finishReducePhase(processingId, nodeAddress, result.processedPartitions());
            } else {
                try {
                    var remoteNode = RmiUtil.getRemoteNode(masterNode);
                    remoteNode.remoteFinishReducePhase(processingId, nodeAddress, result.processedPartitions());
                } catch (RemoteException | RemoteNodeUnavailableException e) {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish reduce phase on %s, master node unavailable".formatted(masterNode), e);
                    cleanup(processingId);
                }
            }
        })
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Reduce phase failed", e);
                    var processingInfo = processingInfos.get(processingId);
                    var masterNode = processingInfo.masterNode();
                    var nodeAddress = Application.getNodeAddress();
                    if (masterNode.equals(nodeAddress)) {
                        // TODO cancel processing on other nodes
                    } else {
                        cleanup(processingId);
                        try {
                            var remoteNode = RmiUtil.getRemoteNode(masterNode);
                            remoteNode.remoteHandleNodeFailure(processingId, nodeAddress);
                        } catch (RemoteException | RemoteNodeUnavailableException e2) {
                            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                    "Reduce phase failed on %s and master node unavailable".formatted(masterNode), e2);
                        }
                    }
                    return null;
                });
    }

    @Override
    public void remoteFinishReducePhase(int processingId, String node, List<Integer> processedPartitions)
            throws RemoteException {
        finishReducePhase(processingId, node, processedPartitions);
    }

    private void finishReducePhase(int processingId, String node, List<Integer> processedPartitions) {
        CompletableFuture.runAsync(() -> {
            processingStates.compute(processingId,
                    (_, processingState) -> {
                        var newState = processingState.addProcessedPartitions(processedPartitions);
                        if (newState.isReducePhaseCompleted()) {
                            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                                    "Reduce phase completed on all nodes");
                            newState = newState.updateStatus(ProcessingStatus.FINISHED);
                            newState.activeNodes().forEach(activeNode -> {
                                if (activeNode.equals(Application.getNodeAddress())) {
                                    finishProcessing(processingId);
                                } else {
                                    try {
                                        var remoteNode = RmiUtil.getRemoteNode(activeNode);
                                        remoteNode.remoteFinishProcessing(processingId);
                                    } catch (RemoteException | RemoteNodeUnavailableException e) {
                                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                                "Failed to finish processing on %s".formatted(node), e);
                                    }
                                }
                            });
                        }
                        return newState;
                    });

        })
                .exceptionally(e -> {
                    // TODO
                    return null;
                });
    }

    @Override
    public void remoteHandleNodeFailure(int processingId, String failedNode) throws RemoteException {
        handleNodeFailure(processingId, failedNode);
    }

    private void handleNodeFailure(int processingId, String failedNode) {
        CompletableFuture.runAsync(() -> {
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Node %s failed".formatted(failedNode));
            processingStates.computeIfPresent(processingId,
                    (_, processingState) -> {
                        if (processingState.activeNodes().contains(failedNode)) {
                            var newState = processingState.removeNode(failedNode);
                            if (!newState.isMapPhaseCompleted()) {
                                var fileAssignments = newState.fileAssignments().get(failedNode);
                                var redistributedFileAssignments = workDistributor.redistributeFiles(processingId,
                                        newState.activeNodes(), fileAssignments);
                                newState = newState.updateFileAssignments(redistributedFileAssignments)
                                        .removeNodeAssignments(failedNode);
                                redistributedFileAssignments.forEach((node, files) -> {
                                    if (node.equals(Application.getNodeAddress())) {
                                        startMapPhase(processingId, files);
                                    } else {
                                        try {
                                            var remoteNode = RmiUtil.getRemoteNode(node);
                                            remoteNode.remoteStartMapPhase(processingId, files);
                                        } catch (RemoteException | RemoteNodeUnavailableException e) {
                                            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                                    "Failed to start map phase on %s".formatted(node), e);
                                            handleNodeFailure(processingId, node);
                                        }
                                    }
                                });
                            } else if (!newState.isReducePhaseCompleted()) {
                                var partitionAssignments = newState.partitionAssignments().get(failedNode);
                                var redistributedPartitionAssignments = workDistributor.redistributePartitions(
                                        processingId,
                                        newState.activeNodes(), partitionAssignments);
                                newState = newState.updatePartitionAssignments(redistributedPartitionAssignments)
                                        .removeNodeAssignments(failedNode);
                                redistributedPartitionAssignments.forEach((node, partitions) -> {
                                    if (node.equals(Application.getNodeAddress())) {
                                        startReducePhase(processingId, partitions);
                                    } else {
                                        try {
                                            var remoteNode = RmiUtil.getRemoteNode(node);
                                            remoteNode.remoteStartReducePhase(processingId, partitions);
                                        } catch (RemoteException | RemoteNodeUnavailableException e) {
                                            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                                    "Failed to start reduce phase on %s".formatted(node), e);
                                            handleNodeFailure(processingId, node);
                                        }
                                    }
                                });
                            }
                            return newState;
                        }
                        return processingState;
                    });
        })
                .exceptionally(e ->
                // e has type CompletionException
                // TODO handle failure in starting reduce phase on master
                null);
    }

    @Override
    public void remoteFinishProcessing(int processingId) throws RemoteException {
        finishProcessing(processingId);
    }

    private void finishProcessing(int processingId) {
        CompletableFuture.runAsync(() -> {
            cleanup(processingId);
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Processing completed successfully");
        })
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish processing gracefully", e);
                    return null;
                });
    }

    private void cleanup(int processingId) {
        var processingInfo = processingInfos.remove(processingId);
        var nodeAddress = Application.getNodeAddress();
        if (nodeAddress.equals(processingInfo.masterNode())) {
            try {
                FilesUtil.removePublicDirectories(processingId);
            } catch (IOException e) {
                LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        "Failed to remove public directories", e);
            }
        }
        try {
            FilesUtil.removeNodeDirectories(processingId);
        } catch (IOException e) {
            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                    "Failed to remove node directories", e);
        }
    }

    private void failProcessing(int processingId) {
        // TODO
    }

    @Override
    public int getProcessingPower() throws RemoteException {
        return Runtime.getRuntime().availableProcessors();
    }

    public ProcessingStatus getProcessingStatus(int processingId) {
        if (!processingStates.containsKey(processingId)) {
            return ProcessingStatus.NOT_FOUND;
        }
        return processingStates.get(processingId).status();
    }

    @Override
    public void isAlive() throws RemoteException {
    }
}

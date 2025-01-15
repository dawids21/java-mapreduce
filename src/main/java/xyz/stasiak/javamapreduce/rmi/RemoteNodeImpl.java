package xyz.stasiak.javamapreduce.rmi;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
                    .updateFileAssignments(fileAssignments);
            processingStates.put(processingId, processingState);

            var masterNode = Application.getNodeAddress();

            activeNodes.forEach(node -> {
                try {
                    var files = processingState.fileAssignments().get(node);
                    if (node.equals(masterNode)) {
                        startNodeProcessing(processingId, parameters, partitionFunction, masterNode);
                        startMapPhase(processingId, files);
                        processingStates.compute(processingId,
                                (_, state) -> state.updateStatus(ProcessingStatus.MAPPING));
                    } else {
                        var remoteNode = RmiUtil.getRemoteNode(node);
                        remoteNode.startNodeProcessing(processingId, parameters, partitionFunction, masterNode);
                        remoteNode.startMapPhase(processingId, files);
                    }
                    // } catch (RemoteException e) {
                    // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                    // "Failed to map phase on %s".formatted(node), e);
                    // throw new CompletionException(e);
                    // // TODO handle failure, node may be dead, maybe repackage in
                    // ProcessingException
                    // } catch (ProcessingException e) {
                    // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                    // "Failed to map phase on %s".formatted(node), e);
                    // throw new CompletionException(e);
                    // // TODO handle failure stop if master node, redistribute if not, probably in
                    // exceptionally block
                } catch (Exception e) {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to map phase on %s".formatted(node), e);
                    throw new CompletionException(e);
                    // TODO handle failure, node may be dead, this probably in exceptionally block
                    // too
                }
            });
        })
                .exceptionally(e -> {
                    // TODO handle failure, node may be dead, this probably in exceptionally block
                    return null;
                });
    }

    @Override
    public void startNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction, String masterNode) throws RemoteException {
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
    public void startMapPhase(int processingId, List<String> files) throws RemoteException {
        CompletableFuture.runAsync(() -> {
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Starting map phase");
            var processingInfo = processingInfos.get(processingId);
            var coordinator = new MapPhaseCoordinator(processingId, processingInfo.parameters().mapperClassName(),
                    processingInfo.parameters().inputDirectory(), files, processingInfo.partitionFunction());
            var result = coordinator.execute();
            var nodeAddress = Application.getNodeAddress();
            var masterNode = processingInfo.masterNode();
            try {
                var remoteNode = RmiUtil.getRemoteNode(masterNode);
                remoteNode.finishMapPhase(processingId, nodeAddress, result.processedFiles());
            } catch (RemoteException | RemoteNodeUnavailableException e) {
                // TODO master unavailable, handle
                LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        "Map phase failed", e);
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
                        try {
                            var remoteNode = RmiUtil.getRemoteNode(masterNode);
                            remoteNode.handleNodeFailure(processingId, nodeAddress);
                            cleanup(processingId);
                        } catch (RemoteException | RemoteNodeUnavailableException e2) {
                            // TODO master unavailable, handle
                            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                    "Map phase failed", e2);
                        }
                    }
                    return null;
                });
    }

    @Override
    public void finishMapPhase(int processingId, String node, int processedFiles) throws RemoteException {
        CompletableFuture.runAsync(() -> {
            var state = processingStates.compute(processingId,
                    (_, processingState) -> processingState.addProcessedFiles(processedFiles));

            if (state.isMapPhaseCompleted()) {
                LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                        "Map phase completed on all nodes");
                try {
                    FilesUtil.removeEmptyPartitionDirectories(processingId);
                } catch (IOException e) {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to remove empty partition directories", e);
                    throw new ProcessingException("Failed to remove empty partition directories", e);
                }

                var updatedState = processingStates.compute(processingId, (_, processingState) -> {
                    var activeNodes = processingState.activeNodes();
                    var partitionAssignments = workDistributor.distributePartitions(
                            processingId, activeNodes);
                    return processingState.updatePartitionAssignments(partitionAssignments);
                });

                updatedState.activeNodes().forEach(activeNode -> {
                    try {
                        var partitions = updatedState.partitionAssignments().get(activeNode);
                        if (activeNode.equals(Application.getNodeAddress())) {
                            startReducePhase(processingId, partitions);
                            processingStates.compute(processingId,
                                    (_, processingState) -> processingState.updateStatus(ProcessingStatus.REDUCING));
                        } else {
                            var remoteNode = RmiUtil.getRemoteNode(activeNode);
                            remoteNode.startReducePhase(processingId, partitions);
                        }
                        // } catch (RemoteException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure, node may be dead, maybe repackage in
                        // ProcessingException
                        // } catch (ProcessingException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure stop if master node, redistribute if not, probably in
                        // exceptionally block
                    } catch (Exception e) {
                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                "Failed to map phase on %s".formatted(node), e);
                        throw new CompletionException(e);
                        // TODO handle failure, node may be dead, this probably in exceptionally block
                        // too
                    }
                });
            }
        })
                .exceptionally(e -> {
                    // e has type CompletionException
                    // TODO handle failure in starting reduce phase on master
                    throw new CompletionException(e);
                });
    }

    @Override
    public void startReducePhase(int processingId, List<Integer> partitions)
            throws RemoteException {
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
            try {
                var remoteNode = RmiUtil.getRemoteNode(masterNode);
                remoteNode.finishReducePhase(processingId, nodeAddress, result.processedPartitions());
            } catch (RemoteException | RemoteNodeUnavailableException e) {
                // TODO master unavailable, handle
                LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        "Failed to finish reduce phase on %s".formatted(masterNode), e);
                throw new CompletionException(e);
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
                        try {
                            var remoteNode = RmiUtil.getRemoteNode(masterNode);
                            remoteNode.handleNodeFailure(processingId, nodeAddress);
                            cleanup(processingId);
                        } catch (RemoteException | RemoteNodeUnavailableException e2) {
                            // TODO master unavailable, handle
                            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                    "Reduce phase failed", e2);
                        }
                    }
                    return null;
                });
    }

    @Override
    public void finishReducePhase(int processingId, String node, int processedPartitions)
            throws RemoteException {
        CompletableFuture.runAsync(() -> {
            var state = processingStates.compute(processingId,
                    (_, processingState) -> processingState.addProcessedPartitions(processedPartitions));

            if (state.isReducePhaseCompleted()) {
                LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                        "Reduce phase completed on all nodes");
                var updatedState = processingStates.compute(processingId,
                        (_, processingState) -> processingState.updateStatus(ProcessingStatus.FINISHED));
                updatedState.activeNodes().forEach(activeNode -> CompletableFuture.runAsync(() -> {
                    try {
                        if (activeNode.equals(Application.getNodeAddress())) {
                            processingStates.compute(processingId,
                                    (_, processingState) -> processingState.updateStatus(ProcessingStatus.FINISHED));
                            finishProcessing(processingId);
                        } else {
                            var remoteNode = RmiUtil.getRemoteNode(activeNode);
                            remoteNode.finishProcessing(processingId);
                        }
                        // } catch (RemoteException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure, node may be dead, maybe repackage in
                        // ProcessingException
                        // } catch (ProcessingException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure stop if master node, redistribute if not, probably in
                        // exceptionally block
                    } catch (Exception e) {
                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                "Failed to reduce phase on %s".formatted(node), e);
                        throw new CompletionException(e);
                        // TODO handle failure, node may be dead, this probably in exceptionally block
                        // too
                    }
                }));
            }
        });
    }

    @Override
    public void handleNodeFailure(int processingId, String failedNode) throws RemoteException {
        CompletableFuture.runAsync(() -> {
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Node %s failed".formatted(failedNode));
            AtomicBoolean redistribute = new AtomicBoolean(false);
            var updatedState = processingStates.computeIfPresent(processingId,
                    (_, processingState) -> {
                        var result = processingState.removeNode(failedNode);
                        if (result.success()) {
                            redistribute.set(true);
                        }
                        return result.newState();
                    });
            if (updatedState == null || !redistribute.get()) {
                return;
            }
            if (!updatedState.isMapPhaseCompleted()) {
                var fileAssignments = updatedState.fileAssignments().get(failedNode);
                var redistributedFileAssignments = workDistributor.redistributeFiles(processingId,
                        updatedState.activeNodes(), fileAssignments);
                var redistributedState = processingStates.compute(processingId,
                        (_, processingState) -> processingState.updateFileAssignments(redistributedFileAssignments)
                                .removeNodeAssignments(failedNode));
                redistributedState.activeNodes().forEach(node -> {
                    try {
                        var files = redistributedFileAssignments.get(node);
                        if (node.equals(Application.getNodeAddress())) {
                            startMapPhase(processingId, files);
                        } else {
                            var remoteNode = RmiUtil.getRemoteNode(node);
                            remoteNode.startMapPhase(processingId, files);
                        }
                        // } catch (RemoteException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure, node may be dead, maybe repackage in
                        // ProcessingException
                        // } catch (ProcessingException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure stop if master node, redistribute if not, probably in
                        // exceptionally block
                    } catch (Exception e) {
                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                "Failed to map phase on %s".formatted(node), e);
                        throw new CompletionException(e);
                        // TODO handle failure, node may be dead, this probably in exceptionally block
                        // too
                    }
                });
            } else if (!updatedState.isReducePhaseCompleted()) {
                var partitionAssignments = updatedState.partitionAssignments().get(failedNode);
                var redistributedPartitionAssignments = workDistributor.redistributePartitions(processingId,
                        updatedState.activeNodes(), partitionAssignments);
                var redistributedState = processingStates.compute(processingId,
                        (_, processingState) -> processingState
                                .updatePartitionAssignments(redistributedPartitionAssignments)
                                .removeNodeAssignments(failedNode));
                redistributedState.activeNodes().forEach(node -> {
                    try {
                        var partitions = redistributedPartitionAssignments.get(node);
                        if (node.equals(Application.getNodeAddress())) {
                            startReducePhase(processingId, partitions);
                        } else {
                            var remoteNode = RmiUtil.getRemoteNode(node);
                            remoteNode.startReducePhase(processingId, partitions);
                        }
                        // } catch (RemoteException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure, node may be dead, maybe repackage in
                        // ProcessingException
                        // } catch (ProcessingException e) {
                        // LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                        // "Failed to map phase on %s".formatted(node), e);
                        // throw new CompletionException(e);
                        // // TODO handle failure stop if master node, redistribute if not, probably in
                        // exceptionally block
                    } catch (Exception e) {
                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                "Failed to map phase on %s".formatted(node), e);
                        throw new CompletionException(e);
                        // TODO handle failure, node may be dead, this probably in exceptionally block
                        // too
                    }
                });
            }
        });
    }

    @Override
    public void finishProcessing(int processingId) throws RemoteException {
        CompletableFuture.runAsync(() -> {
            cleanup(processingId);
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Processing completed successfully");
        })
                .exceptionally(e -> {
                    // e has type CompletionException
                    // TODO handle failure in starting reduce phase on master
                    throw new CompletionException(e);
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

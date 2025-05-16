package xyz.stasiak.javamapreduce.processing;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.map.MapPhaseCoordinator;
import xyz.stasiak.javamapreduce.reduce.ReducePhaseCoordinator;
import xyz.stasiak.javamapreduce.rmi.RemoteController;
import xyz.stasiak.javamapreduce.rmi.RemoteNodeUnavailableException;
import xyz.stasiak.javamapreduce.rmi.RemoteRuntimeException;
import xyz.stasiak.javamapreduce.rmi.RmiUtil;
import xyz.stasiak.javamapreduce.util.FilesUtil;
import xyz.stasiak.javamapreduce.util.LoggingUtil;
import xyz.stasiak.javamapreduce.util.RuntimeUtil;
import xyz.stasiak.javamapreduce.util.SystemProperties;

public class Controller extends UnicastRemoteObject implements RemoteController {

    private static final Logger LOGGER = Logger.getLogger(Controller.class.getSimpleName());
    private static final String NODE_ADDRESS = SystemProperties.getNodeAddress();
    private static final double PROCESSING_POWER_MULTIPLIER = SystemProperties.processingPowerMultiplier();

    private final Map<Integer, ProcessingState> processingStates;
    private final Map<Integer, ProcessingInfo> processingInfos;
    private final WorkDistributor workDistributor = new WorkDistributor();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService healthCheckExecutor = Executors.newSingleThreadScheduledExecutor();

    public Controller() throws RemoteException {
        super(Integer.parseInt(SystemProperties.getRmiPort()) + 1);
        this.processingStates = new ConcurrentHashMap<>();
        this.processingInfos = new ConcurrentHashMap<>();
        healthCheckExecutor.scheduleWithFixedDelay(
                () -> checkNodesHealth(),
                10,
                10,
                TimeUnit.SECONDS);
    }

    public void startProcessing(int processingId, ProcessingParameters parameters) {
        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "Starting processing at: " + System.currentTimeMillis());
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

            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Starting map phase at: " + System.currentTimeMillis());
            activeNodes.forEach(node -> {
                var files = processingState.fileAssignments().get(node);
                if (node.equals(NODE_ADDRESS)) {
                    startNodeProcessing(processingId, parameters, partitionFunction, NODE_ADDRESS);
                    processingStates.compute(processingId,
                            (_, state) -> state.updateStatus(ProcessingStatus.MAPPING));
                    startMapPhase(processingId, files);
                } else {
                    try {
                        RmiUtil.call(node, (remoteController) -> {
                            try {
                                remoteController.remoteStartNodeProcessing(processingId, parameters, partitionFunction,
                                        NODE_ADDRESS);
                                remoteController.remoteStartMapPhase(processingId, files);
                            } catch (RemoteException e) {
                                throw new RemoteRuntimeException(e);
                            }
                        });
                    } catch (RemoteException | RemoteNodeUnavailableException | ProcessingException e) {
                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                "Failed to start map phase on %s".formatted(node), e);
                        handleNodeFailure(processingId, node);
                    }
                }
            });
        }, executor)
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
        var processingInfo = ProcessingInfo.create(processingId, parameters, masterNode, partitionFunction);
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
            if (processingInfo == null) {
                LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                        "Processing cancelled");
                return;
            }
            var coordinator = new MapPhaseCoordinator(processingId, processingInfo.parameters().mapperClassName(),
                    processingInfo.parameters().inputDirectory(), files, processingInfo.partitionFunction(),
                    processingInfo.cancellationToken());
            var result = coordinator.execute();
            var masterNode = processingInfo.masterNode();
            if (masterNode.equals(NODE_ADDRESS)) {
                finishMapPhase(processingId, NODE_ADDRESS, result.processedFiles());
            } else {
                try {
                    RmiUtil.call(masterNode, (remoteController) -> {
                        try {
                            remoteController.remoteFinishMapPhase(processingId, NODE_ADDRESS, result.processedFiles());
                        } catch (RemoteException e) {
                            throw new RemoteRuntimeException(e);
                        }
                    });
                } catch (RemoteException | RemoteNodeUnavailableException e) {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish map phase on %s, master node unavailable".formatted(masterNode), e);
                    cleanup(processingId);
                }
            }
        }, executor)
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Map phase failed", e);
                    var processingInfo = processingInfos.get(processingId);
                    var masterNode = processingInfo.masterNode();
                    if (masterNode.equals(NODE_ADDRESS)) {
                        failProcessing(processingId);
                    } else {
                        cleanup(processingId);
                        try {
                            RmiUtil.call(masterNode, (remoteController) -> {
                                try {
                                    remoteController.remoteHandleNodeFailure(processingId, NODE_ADDRESS);
                                } catch (RemoteException e2) {
                                    throw new RemoteRuntimeException(e2);
                                }
                            });
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
                        if (processingState.isFailed()) {
                            if (node.equals(NODE_ADDRESS)) {
                                cleanup(processingId);
                            } else {
                                try {
                                    RmiUtil.call(node, (remoteController) -> {
                                        try {
                                            remoteController.remoteCleanup(processingId);
                                        } catch (RemoteException e) {
                                            throw new RemoteRuntimeException(e);
                                        }
                                    });
                                } catch (RemoteException | RemoteNodeUnavailableException e) {
                                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                            "Failed to cleanup on %s".formatted(node), e);
                                }
                            }
                            return processingState;
                        }
                        var newState = processingState.addProcessedFiles(processedFiles);
                        if (newState.isMapPhaseCompleted()) {
                            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                                    "Finished map phase at: " + System.currentTimeMillis());
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
                            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                                    "Starting reduce phase at: " + System.currentTimeMillis());
                            newState.activeNodes().forEach(activeNode -> {
                                var partitions = partitionAssignments.get(activeNode);
                                if (activeNode.equals(NODE_ADDRESS)) {
                                    startReducePhase(processingId, partitions);
                                } else {
                                    try {
                                        RmiUtil.call(activeNode, (remoteController) -> {
                                            try {
                                                remoteController.remoteStartReducePhase(processingId, partitions);
                                            } catch (RemoteException e) {
                                                throw new RemoteRuntimeException(e);
                                            }
                                        });
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
        }, executor)
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish map phase and start reduce phase", e);
                    failProcessing(processingId);
                    return null;
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
            if (processingInfo == null) {
                LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                        "Processing cancelled");
                return;
            }
            var coordinator = new ReducePhaseCoordinator(processingId, processingInfo.parameters().reducerClassName(),
                    partitions, processingInfo.parameters().outputDirectory(), processingInfo.cancellationToken());
            var result = coordinator.execute();
            var masterNode = processingInfo.masterNode();
            if (masterNode.equals(NODE_ADDRESS)) {
                finishReducePhase(processingId, NODE_ADDRESS, result.processedPartitions());
            } else {
                try {
                    RmiUtil.call(masterNode, (remoteController) -> {
                        try {
                            remoteController.remoteFinishReducePhase(processingId, NODE_ADDRESS,
                                    result.processedPartitions());
                        } catch (RemoteException e) {
                            throw new RemoteRuntimeException(e);
                        }
                    });
                } catch (RemoteException | RemoteNodeUnavailableException e) {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish reduce phase on %s, master node unavailable".formatted(masterNode), e);
                    cleanup(processingId);
                }
            }
        }, executor)
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Reduce phase failed", e);
                    var processingInfo = processingInfos.get(processingId);
                    var masterNode = processingInfo.masterNode();
                    if (masterNode.equals(NODE_ADDRESS)) {
                        failProcessing(processingId);
                    } else {
                        cleanup(processingId);
                        try {
                            RmiUtil.call(masterNode, (remoteController) -> {
                                try {
                                    remoteController.remoteHandleNodeFailure(processingId, NODE_ADDRESS);
                                } catch (RemoteException e2) {
                                    throw new RemoteRuntimeException(e2);
                                }
                            });
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
                        if (processingState.isFailed()) {
                            if (node.equals(NODE_ADDRESS)) {
                                cleanup(processingId);
                            } else {
                                try {
                                    RmiUtil.call(node, (remoteController) -> {
                                        try {
                                            remoteController.remoteCleanup(processingId);
                                        } catch (RemoteException e) {
                                            throw new RemoteRuntimeException(e);
                                        }
                                    });
                                } catch (RemoteException | RemoteNodeUnavailableException e) {
                                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                            "Failed to cleanup on %s".formatted(node), e);
                                }
                            }
                            return processingState;
                        }
                        var newState = processingState.addProcessedPartitions(processedPartitions);
                        if (newState.isReducePhaseCompleted()) {
                            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                                    "Finished reduce phase at: " + System.currentTimeMillis());
                            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                                    "Reduce phase completed on all nodes");
                            newState = newState.updateStatus(ProcessingStatus.FINISHED);
                            newState.activeNodes().forEach(activeNode -> {
                                if (activeNode.equals(NODE_ADDRESS)) {
                                    finishProcessing(processingId);
                                } else {
                                    try {
                                        RmiUtil.call(activeNode, (remoteController) -> {
                                            try {
                                                remoteController.remoteFinishProcessing(processingId);
                                            } catch (RemoteException e) {
                                                throw new RemoteRuntimeException(e);
                                            }
                                        });
                                    } catch (RemoteException | RemoteNodeUnavailableException e) {
                                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                                "Failed to finish processing on %s".formatted(node), e);
                                    }
                                }
                            });
                        }
                        return newState;
                    });

        }, executor)
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish reduce phase", e);
                    failProcessing(processingId);
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
                        if (processingState.isFailed()) {
                            return processingState;
                        }
                        if (processingState.activeNodes().contains(failedNode)) {
                            var newState = processingState.removeNode(failedNode);
                            if (!newState.isMapPhaseCompleted()) {
                                var fileAssignments = newState.fileAssignments().get(failedNode);
                                var redistributedFileAssignments = workDistributor.redistributeFiles(processingId,
                                        newState.activeNodes(), fileAssignments);
                                newState = newState.updateFileAssignments(redistributedFileAssignments)
                                        .removeNodeAssignments(failedNode);
                                redistributedFileAssignments.forEach((node, files) -> {
                                    if (node.equals(NODE_ADDRESS)) {
                                        startMapPhase(processingId, files);
                                    } else {
                                        try {
                                            RmiUtil.call(node, (remoteController) -> {
                                                try {
                                                    remoteController.remoteStartMapPhase(processingId, files);
                                                } catch (RemoteException e) {
                                                    throw new RemoteRuntimeException(e);
                                                }
                                            });
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
                                    if (node.equals(NODE_ADDRESS)) {
                                        startReducePhase(processingId, partitions);
                                    } else {
                                        try {
                                            RmiUtil.call(node, (remoteController) -> {
                                                try {
                                                    remoteController.remoteStartReducePhase(processingId, partitions);
                                                } catch (RemoteException e) {
                                                    throw new RemoteRuntimeException(e);
                                                }
                                            });
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
        }, executor)
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to handle node failure", e);
                    failProcessing(processingId);
                    return null;
                });
    }

    @Override
    public void remoteFinishProcessing(int processingId) throws RemoteException {
        finishProcessing(processingId);
    }

    private void finishProcessing(int processingId) {
        CompletableFuture.runAsync(() -> {
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Finishing processing at: " + System.currentTimeMillis());
            cleanup(processingId);
            LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                    "Processing completed successfully");
        }, executor)
                .exceptionally(e -> {
                    LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                            "Failed to finish processing gracefully", e);
                    return null;
                });
    }

    private void failProcessing(int processingId) {
        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                "Processing failed");
        processingStates.compute(processingId, (_, processingState) -> {
            var newState = processingState.fail();
            newState.activeNodes().forEach(node -> {
                if (node.equals(NODE_ADDRESS)) {
                    cleanup(processingId);
                } else {
                    try {
                        RmiUtil.call(node, (remoteController) -> {
                            try {
                                remoteController.remoteCleanup(processingId);
                            } catch (RemoteException e) {
                                throw new RemoteRuntimeException(e);
                            }
                        });
                    } catch (RemoteException | RemoteNodeUnavailableException e) {
                        LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                                "Failed to cleanup on %s".formatted(node), e);
                    }
                }
            });
            return newState;
        });
    }

    @Override
    public void remoteCleanup(int processingId) throws RemoteException {
        cleanup(processingId);
    }

    private void cleanup(int processingId) {
        CompletableFuture.runAsync(() -> {
            if (!processingInfos.containsKey(processingId)) {
                return;
            }
            var processingInfo = processingInfos.remove(processingId);

            processingInfo.cancellationToken().cancel();

            if (NODE_ADDRESS.equals(processingInfo.masterNode())) {
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
        }, executor);
    }

    @Override
    public int getProcessingPower() throws RemoteException {
        return RuntimeUtil.getProcessingPower(PROCESSING_POWER_MULTIPLIER);
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

    private void checkNodesHealth() {
        var knownNodes = SystemProperties.getKnownNodes();
        knownNodes.stream()
                .filter(node -> !node.equals(NODE_ADDRESS))
                .forEach(node -> {
                    try {
                        RmiUtil.call(node, (remoteController) -> {
                            try {
                                remoteController.isAlive();
                            } catch (RemoteException e) {
                                throw new RemoteRuntimeException(e);
                            }
                        });
                    } catch (RemoteException | RemoteRuntimeException | RemoteNodeUnavailableException e) {
                        processingInfos.forEach((processingId, processingInfo) -> {
                            if (processingInfo.masterNode().equals(node)) {
                                cleanup(processingId);
                            }
                        });
                        processingStates.forEach((processingId, state) -> {
                            if (state.activeNodes().contains(node)) {
                                handleNodeFailure(processingId, node);
                            }
                        });
                    }
                });
    }

    public void shutdownExecutor() {
        LoggingUtil.logInfo(LOGGER, getClass(), "Shutting down executor services");
        executor.close();
        healthCheckExecutor.shutdown();
    }
}

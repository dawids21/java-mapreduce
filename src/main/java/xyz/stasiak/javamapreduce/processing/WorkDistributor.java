package xyz.stasiak.javamapreduce.processing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.map.PartitionFunction;
import xyz.stasiak.javamapreduce.rmi.RemoteNodeUnavailableException;
import xyz.stasiak.javamapreduce.rmi.RemoteRuntimeException;
import xyz.stasiak.javamapreduce.rmi.RmiUtil;
import xyz.stasiak.javamapreduce.util.FilesUtil;
import xyz.stasiak.javamapreduce.util.LoggingUtil;
import xyz.stasiak.javamapreduce.util.RuntimeUtil;
import xyz.stasiak.javamapreduce.util.SystemProperties;

class WorkDistributor {
    private static final Logger LOGGER = Logger.getLogger(WorkDistributor.class.getName());
    private static final String NODE_ADDRESS = SystemProperties.getNodeAddress();
    private static final List<String> KNOWN_NODES = SystemProperties.getKnownNodes();

    private record NodeInfo(String address, int processingPower) {
        static NodeInfo fromRemoteNode(String address) throws RemoteException, RemoteNodeUnavailableException {
            Integer result = RmiUtil.call(address, (remoteController) -> {
                try {
                    return Integer.valueOf(remoteController.getProcessingPower());
                } catch (RemoteException e) {
                    throw new RemoteRuntimeException(e);
                }
            });
            return new NodeInfo(address, result);
        }

        static NodeInfo fromLocalNode(String address) {
            return new NodeInfo(address, RuntimeUtil.getProcessingPower());
        }
    }

    List<String> getActiveNodes(int processingId) {
        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "Starting node discovery for active nodes");

        var activeNodes = new ArrayList<String>();
        for (var nodeAddress : KNOWN_NODES) {
            try {
                if (NODE_ADDRESS.equals(nodeAddress)) {
                    activeNodes.add(nodeAddress);
                    continue;
                }
                RmiUtil.call(nodeAddress, (remoteController) -> {
                    try {
                        remoteController.isAlive();
                    } catch (RemoteException e) {
                        throw new RemoteRuntimeException(e);
                    }
                });
                activeNodes.add(nodeAddress);
            } catch (RemoteNodeUnavailableException | RemoteException e) {
                LoggingUtil.logWarning(LOGGER, processingId, getClass(),
                        "Node %s is not available".formatted(nodeAddress), e);
            }
        }

        return activeNodes;
    }

    Map<String, List<String>> distributeFiles(int processingId, List<String> activeNodes, String inputDirectory) {
        List<String> files;
        try {
            files = Files.list(Path.of(inputDirectory)).map(path -> path.getFileName().toString()).toList();
        } catch (IOException e) {
            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                    "Failed to list files in directory %s".formatted(inputDirectory), e);
            throw new ProcessingException("Failed to list files in directory", e);
        }
        return redistributeFiles(processingId, activeNodes, files);
    }

    Map<String, List<String>> redistributeFiles(int processingId, List<String> activeNodes, List<String> files) {
        var nodesWithPower = getActiveNodesWithPower(processingId, activeNodes);
        var totalPower = nodesWithPower.values().stream()
                .mapToInt(NodeInfo::processingPower)
                .sum();

        var result = new HashMap<String, List<String>>();
        var remainingFiles = new ArrayList<>(files);
        var totalFiles = files.size();

        for (var entry : nodesWithPower.entrySet()) {
            var nodeAddress = entry.getKey();
            var nodeInfo = entry.getValue();

            var powerRatio = (double) nodeInfo.processingPower() / totalPower;
            var fileCount = (int) Math.ceil(totalFiles * powerRatio);
            var nodeFiles = new ArrayList<String>();

            var nodeFilesCount = Math.min(fileCount, remainingFiles.size());
            for (int i = 0; i < nodeFilesCount; i++) {
                nodeFiles.add(remainingFiles.get(i));
            }
            remainingFiles.subList(0, nodeFilesCount).clear();

            result.put(nodeAddress, nodeFiles);
        }

        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "File assignments: %s".formatted(result));

        return result;
    }

    Map<String, List<Integer>> distributePartitions(int processingId, List<String> activeNodes) {
        List<Integer> partitionsToDistribute;
        try {
            partitionsToDistribute = FilesUtil.getPartitions(processingId);
        } catch (IOException e) {
            LoggingUtil.logSevere(LOGGER, processingId, getClass(),
                    "Failed to get partitions for processing", e);
            throw new ProcessingException("Failed to get partitions for processing", e);
        }
        return redistributePartitions(processingId, activeNodes, partitionsToDistribute);
    }

    Map<String, List<Integer>> redistributePartitions(int processingId, List<String> activeNodes,
            List<Integer> partitions) {
        var remainingPartitions = new ArrayList<Integer>(partitions);
        var nodesWithPower = getActiveNodesWithPower(processingId, activeNodes);
        var totalPower = nodesWithPower.values().stream()
                .mapToInt(NodeInfo::processingPower)
                .sum();

        var totalPartitions = partitions.size();
        var result = new HashMap<String, List<Integer>>();
        for (var entry : nodesWithPower.entrySet()) {
            var nodeAddress = entry.getKey();
            var nodeInfo = entry.getValue();

            var powerRatio = (double) nodeInfo.processingPower() / totalPower;
            var partitionCount = (int) Math.ceil(totalPartitions * powerRatio);
            var nodePartitions = new ArrayList<Integer>();

            var nodePartitionCount = Math.min(partitionCount, remainingPartitions.size());
            for (int i = 0; i < nodePartitionCount; i++) {
                nodePartitions.add(remainingPartitions.get(i));
            }
            remainingPartitions.subList(0, nodePartitionCount).clear();

            result.put(nodeAddress, nodePartitions);
        }

        LoggingUtil.logInfo(LOGGER, processingId, getClass(),
                "Partition assignments: %s".formatted(result));

        return result;
    }

    int calculateTotalPartitions(int processingId, List<String> activeNodes) {
        var activeNodesWithPower = getActiveNodesWithPower(processingId, activeNodes);
        var totalPower = activeNodesWithPower.values().stream()
                .mapToInt(NodeInfo::processingPower)
                .sum();

        return totalPower;
    }

    Function<String, Integer> createPartitionFunction(int partitionCount) {
        return new PartitionFunction(partitionCount);
    }

    private Map<String, NodeInfo> getActiveNodesWithPower(int processingId, List<String> activeNodes) {
        var activeNodesWithPower = new HashMap<String, NodeInfo>();

        for (var nodeAddress : activeNodes) {
            try {
                if (NODE_ADDRESS.equals(nodeAddress)) {
                    var nodeInfo = NodeInfo.fromLocalNode(nodeAddress);
                    activeNodesWithPower.put(nodeAddress, nodeInfo);
                    continue;
                }
                var nodeInfo = NodeInfo.fromRemoteNode(nodeAddress);
                activeNodesWithPower.put(nodeAddress, nodeInfo);
            } catch (RemoteException | RemoteNodeUnavailableException e) {
                LoggingUtil.logWarning(LOGGER, processingId, getClass(),
                        "Node %s is not available".formatted(nodeAddress), e);
                var nodeInfo = new NodeInfo(nodeAddress, 0);
                activeNodesWithPower.put(nodeAddress, nodeInfo);
            }
        }

        return activeNodesWithPower;
    }
}
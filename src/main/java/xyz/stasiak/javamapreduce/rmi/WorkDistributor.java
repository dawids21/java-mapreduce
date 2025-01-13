package xyz.stasiak.javamapreduce.rmi;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.Application;
import xyz.stasiak.javamapreduce.util.FilesUtil;

class WorkDistributor {
    private static final Logger LOGGER = Logger.getLogger(WorkDistributor.class.getName());

    private record NodeInfo(String address, int processingPower) {
        static NodeInfo fromRemoteNode(String address, RemoteNode node) throws RemoteException {
            return new NodeInfo(address, node.getProcessingPower());
        }
    }

    List<String> getActiveNodes(int processingId) throws RemoteException {
        var knownNodes = Application.getKnownNodes();
        LOGGER.info("(%d) [%s] Starting node discovery for active nodes".formatted(processingId,
                this.getClass().getSimpleName()));

        var activeNodes = new ArrayList<String>();
        for (var nodeAddress : knownNodes) {
            try {
                var node = (RemoteNode) Naming.lookup(nodeAddress);
                node.isAlive();
                activeNodes.add(nodeAddress);
            } catch (RemoteException | NotBoundException | MalformedURLException e) {
                LOGGER.warning("(%d) [%s] Node %s is not available: %s".formatted(processingId,
                        this.getClass().getSimpleName(), nodeAddress, e.getMessage()));
            }
        }

        return activeNodes;
    }

    Map<String, List<String>> distributeFiles(int processingId, List<String> activeNodes, String inputDirectory)
            throws RemoteException {
        var nodesWithPower = getActiveNodesWithPower(processingId, activeNodes);
        var totalPower = nodesWithPower.values().stream()
                .mapToInt(NodeInfo::processingPower)
                .sum();

        List<String> files;
        try {
            files = Files.list(Path.of(inputDirectory)).map(path -> path.getFileName().toString()).toList();
        } catch (IOException e) {
            LOGGER.severe("(%d) [%s] Failed to list files in directory %s: %s".formatted(
                    processingId, this.getClass().getSimpleName(), inputDirectory, e.getMessage()));
            throw new RuntimeException("Failed to list files in directory", e);
        }

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

        LOGGER.info("(%d) [%s] File assignments: %s".formatted(
                processingId, this.getClass().getSimpleName(), result));

        return result;
    }

    Map<String, List<Integer>> distributePartitions(int processingId, List<String> activeNodes)
            throws RemoteException, IOException {
        var nodesWithPower = getActiveNodesWithPower(processingId, activeNodes);
        var totalPower = nodesWithPower.values().stream()
                .mapToInt(NodeInfo::processingPower)
                .sum();

        var result = new HashMap<String, List<Integer>>();
        var partitionsToDistribute = FilesUtil.getPartitions(processingId);
        var remainingPartitions = new ArrayList<Integer>(partitionsToDistribute);
        var totalPartitions = partitionsToDistribute.size();

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

        LOGGER.info("(%d) [%s] Partition assignments: %s".formatted(
                processingId, this.getClass().getSimpleName(), result));

        return result;
    }

    int calculateTotalPartitions(int processingId, List<String> activeNodes) throws RemoteException {
        var activeNodesWithPower = getActiveNodesWithPower(processingId, activeNodes);
        var totalPower = activeNodesWithPower.values().stream()
                .mapToInt(NodeInfo::processingPower)
                .sum();

        return totalPower;
    }

    Function<String, Integer> createPartitionFunction(int partitionCount) {
        return new PartitionFunction(partitionCount);
    }

    private Map<String, NodeInfo> getActiveNodesWithPower(int processingId, List<String> activeNodes)
            throws RemoteException {
        var activeNodesWithPower = new HashMap<String, NodeInfo>();

        for (var nodeAddress : activeNodes) {
            try {
                var node = (RemoteNode) Naming.lookup(nodeAddress);
                var nodeInfo = NodeInfo.fromRemoteNode(nodeAddress, node);
                activeNodesWithPower.put(nodeAddress, nodeInfo);
            } catch (RemoteException | NotBoundException | MalformedURLException e) {
                LOGGER.warning("(%d) [%s] Node %s is not available: %s".formatted(processingId,
                        this.getClass().getSimpleName(), nodeAddress, e.getMessage()));
            }
        }

        return activeNodesWithPower;
    }
}
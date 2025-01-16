package xyz.stasiak.javamapreduce.rmi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

record ProcessingState(
        int processingId,
        List<String> activeNodes,
        ProcessingStatus status,
        Map<String, List<String>> fileAssignments,
        Map<String, List<Integer>> partitionAssignments,
        int processedFiles,
        int processedPartitions) {

    static ProcessingState create(int processingId, List<String> activeNodes) {

        var fileAssignments = new HashMap<String, List<String>>();
        activeNodes.forEach(node -> fileAssignments.put(node, new ArrayList<>()));
        var partitionAssignments = new HashMap<String, List<Integer>>();
        activeNodes.forEach(node -> partitionAssignments.put(node, new ArrayList<>()));

        return new ProcessingState(
                processingId,
                new ArrayList<>(activeNodes),
                ProcessingStatus.NOT_STARTED,
                fileAssignments,
                partitionAssignments,
                0,
                0);
    }

    ProcessingState updateStatus(ProcessingStatus status) {
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                partitionAssignments,
                processedFiles,
                processedPartitions);
    }

    ProcessingState addProcessedFiles(int count) {
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                partitionAssignments,
                processedFiles + count,
                processedPartitions);
    }

    ProcessingState addProcessedPartitions(int count) {
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                partitionAssignments,
                processedFiles,
                processedPartitions + count);
    }

    boolean isMapPhaseCompleted() {
        var totalFiles = fileAssignments.values().stream().mapToInt(List::size).sum();
        return processedFiles == totalFiles;
    }

    boolean isReducePhaseCompleted() {
        var totalPartitions = partitionAssignments.values().stream().mapToInt(List::size).sum();
        return processedPartitions == totalPartitions;
    }

    ProcessingState removeNode(String node) {
        var newActiveNodes = new ArrayList<>(activeNodes);
        newActiveNodes.remove(node);
        return new ProcessingState(
                processingId,
                newActiveNodes,
                status,
                fileAssignments,
                partitionAssignments,
                processedFiles,
                processedPartitions);
    }

    ProcessingState updateFileAssignments(Map<String, List<String>> fileAssignments) {
        var newFileAssignments = new HashMap<>(this.fileAssignments);
        this.fileAssignments.forEach((node, files) -> newFileAssignments.put(node, new ArrayList<>(files)));
        fileAssignments.forEach((node, files) -> {
            var assignment = newFileAssignments.get(node);
            if (assignment != null) {
                assignment.addAll(files);
            }
        });
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                newFileAssignments,
                partitionAssignments,
                processedFiles,
                processedPartitions);
    }

    ProcessingState updatePartitionAssignments(Map<String, List<Integer>> partitionAssignments) {
        var newPartitionAssignments = new HashMap<>(this.partitionAssignments);
        this.partitionAssignments
                .forEach((node, partitions) -> newPartitionAssignments.put(node, new ArrayList<>(partitions)));
        partitionAssignments.forEach((node, partitions) -> {
            var assignment = newPartitionAssignments.get(node);
            if (assignment != null) {
                assignment.addAll(partitions);
            }
        });
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                newPartitionAssignments,
                processedFiles,
                processedPartitions);
    }

    ProcessingState removeNodeAssignments(String node) {
        var newFileAssignments = new HashMap<>(fileAssignments);
        newFileAssignments.remove(node);
        var newPartitionAssignments = new HashMap<>(partitionAssignments);
        newPartitionAssignments.remove(node);
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                newFileAssignments,
                newPartitionAssignments,
                processedFiles,
                processedPartitions);
    }
}

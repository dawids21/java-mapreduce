package xyz.stasiak.javamapreduce.rmi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

record ProcessingState(
        int processingId,
        List<String> activeNodes,
        ProcessingStatus status,
        Map<String, NodeAssignment> nodeAssignments,
        Set<String> mapCompletedNodes,
        Set<String> reduceCompletedNodes,
        AtomicInteger processedFiles) {

    static ProcessingState create(int processingId, List<String> activeNodes) {
        var nodeAssignments = activeNodes.stream()
                .collect(Collectors.toMap(
                        node -> node,
                        _ -> new NodeAssignment(new ArrayList<>(), new ArrayList<>())));

        return new ProcessingState(
                processingId,
                new ArrayList<>(activeNodes),
                ProcessingStatus.NOT_STARTED,
                nodeAssignments,
                ConcurrentHashMap.newKeySet(),
                ConcurrentHashMap.newKeySet(),
                new AtomicInteger(0));
    }

    void incrementProcessedFiles() {
        processedFiles.incrementAndGet();
    }

    boolean allFilesProcessed() {
        var totalFiles = nodeAssignments.values().stream().mapToInt(nodeAssignment -> nodeAssignment.files().size()).sum();
        return processedFiles.get() == totalFiles;
    }

    ProcessingState updateStatus(ProcessingStatus status) {
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                nodeAssignments,
                mapCompletedNodes,
                reduceCompletedNodes,
                processedFiles);
    }

    boolean isMapPhaseCompleted() {
        return mapCompletedNodes.size() == activeNodes.size();
    }

    boolean isReducePhaseCompleted() {
        return reduceCompletedNodes.size() == activeNodes.size();
    }

    void updateFileAssignments(Map<String, List<String>> fileAssignments) {
        fileAssignments.forEach((node, files) -> {
            var assignment = nodeAssignments.get(node);
            if (assignment != null) {
                assignment.files().addAll(files);
            }
        });
    }

    void updatePartitionAssignments(Map<String, List<Integer>> partitionAssignments) {
        partitionAssignments.forEach((node, partitions) -> {
            var assignment = nodeAssignments.get(node);
            if (assignment != null) {
                assignment.partitions().addAll(partitions);
            }
        });
    }
}
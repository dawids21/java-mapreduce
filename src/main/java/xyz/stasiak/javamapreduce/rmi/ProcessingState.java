package xyz.stasiak.javamapreduce.rmi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

record ProcessingState(
        int processingId,
        List<String> activeNodes,
        ProcessingStatus status,
        Map<String, NodeAssignment> nodeAssignments,
        int totalFiles,
        int totalPartitions,
        AtomicInteger processedFiles,
        AtomicInteger processedPartitions) {

    static ProcessingState create(int processingId, List<String> activeNodes, int totalFiles, int totalPartitions) {
        var nodeAssignments = activeNodes.stream()
                .collect(Collectors.toMap(
                        node -> node,
                        _ -> new NodeAssignment(new ArrayList<>(), new ArrayList<>())));

        return new ProcessingState(
                processingId,
                new ArrayList<>(activeNodes),
                ProcessingStatus.NOT_STARTED,
                nodeAssignments,
                totalFiles,
                totalPartitions,
                new AtomicInteger(0),
                new AtomicInteger(0));
    }

    ProcessingState updateStatus(ProcessingStatus status) {
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                nodeAssignments,
                totalFiles,
                totalPartitions,
                processedFiles,
                processedPartitions);
    }

    void addProcessedFiles(int count) {
        processedFiles.addAndGet(count);
    }

    void addProcessedPartitions(int count) {
        processedPartitions.addAndGet(count);
    }

    boolean isMapPhaseCompleted() {
        return processedFiles.get() == totalFiles;
    }

    boolean isReducePhaseCompleted() {
        return processedPartitions.get() == totalPartitions;
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
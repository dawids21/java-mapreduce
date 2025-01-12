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
        int totalFiles,
        int totalPartitions,
        int processedFiles,
        int processedPartitions) {

    static ProcessingState create(int processingId, List<String> activeNodes, int totalFiles, int totalPartitions) {

        return new ProcessingState(
                processingId,
                new ArrayList<>(activeNodes),
                ProcessingStatus.NOT_STARTED,
                new HashMap<>(),
                new HashMap<>(),
                totalFiles,
                totalPartitions,
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
                totalFiles,
                totalPartitions,
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
                totalFiles,
                totalPartitions,
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
                totalFiles,
                totalPartitions,
                processedFiles,
                processedPartitions + count);
    }

    boolean isMapPhaseCompleted() {
        return processedFiles == totalFiles;
    }

    boolean isReducePhaseCompleted() {
        return processedPartitions == totalPartitions;
    }

    ProcessingState updateFileAssignments(Map<String, List<String>> fileAssignments) {
        var newFileAssignments = new HashMap<>(this.fileAssignments);
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
                totalFiles,
                totalPartitions,
                processedFiles,
                processedPartitions);
    }

        ProcessingState updatePartitionAssignments(Map<String, List<Integer>> partitionAssignments) {
        var newPartitionAssignments = new HashMap<>(this.partitionAssignments);
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
                totalFiles,
                totalPartitions,
                processedFiles,
                processedPartitions);
    }
}

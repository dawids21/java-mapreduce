package xyz.stasiak.javamapreduce.processing;

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
        List<String> remainingFiles,
        List<Integer> remainingPartitions) {

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
                new ArrayList<>(),
                new ArrayList<>());
    }

    ProcessingState updateStatus(ProcessingStatus status) {
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                partitionAssignments,
                remainingFiles,
                remainingPartitions);
    }

    ProcessingState addProcessedFiles(List<String> files) {
        var newRemainingFiles = new ArrayList<>(remainingFiles);
        newRemainingFiles.removeAll(files);
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                partitionAssignments,
                newRemainingFiles,
                remainingPartitions);
    }

    ProcessingState addProcessedPartitions(List<Integer> partitions) {
        var newRemainingPartitions = new ArrayList<>(remainingPartitions);
        newRemainingPartitions.removeAll(partitions);
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                partitionAssignments,
                remainingFiles,
                newRemainingPartitions);
    }

    boolean isMapPhaseCompleted() {
        return remainingFiles.isEmpty();
    }

    boolean isReducePhaseCompleted() {
        return remainingPartitions.isEmpty();
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
                remainingFiles,
                remainingPartitions);
    }

    ProcessingState setFileAssignments(Map<String, List<String>> fileAssignments) {
        var newFileAssignments = new HashMap<>(fileAssignments);
        var newRemainingFiles = new ArrayList<>(fileAssignments.values().stream().flatMap(List::stream).toList());
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                newFileAssignments,
                partitionAssignments,
                newRemainingFiles,
                remainingPartitions);
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
                remainingFiles,
                remainingPartitions);
    }

    ProcessingState setPartitionAssignments(Map<String, List<Integer>> partitionAssignments) {
        var newPartitionAssignments = new HashMap<>(partitionAssignments);
        var newRemainingPartitions = new ArrayList<>(
                partitionAssignments.values().stream().flatMap(List::stream).toList());
        return new ProcessingState(
                processingId,
                activeNodes,
                status,
                fileAssignments,
                newPartitionAssignments,
                remainingFiles,
                newRemainingPartitions);
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
                remainingFiles,
                remainingPartitions);
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
                remainingFiles,
                remainingPartitions);
    }

    ProcessingState fail() {
        if (status == ProcessingStatus.MAPPING) {
            return new ProcessingState(
                    processingId,
                    activeNodes,
                    ProcessingStatus.MAPPING_FAILED,
                    fileAssignments,
                    partitionAssignments,
                    remainingFiles,
                    remainingPartitions);
        } else if (status == ProcessingStatus.REDUCING) {
            return new ProcessingState(
                    processingId,
                    activeNodes,
                    ProcessingStatus.REDUCING_FAILED,
                    fileAssignments,
                    partitionAssignments,
                    remainingFiles,
                    remainingPartitions);
        }
        return this;
    }

    boolean isFailed() {
        return status == ProcessingStatus.MAPPING_FAILED || status == ProcessingStatus.REDUCING_FAILED;
    }
}

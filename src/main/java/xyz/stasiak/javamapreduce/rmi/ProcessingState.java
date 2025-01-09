package xyz.stasiak.javamapreduce.rmi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

record ProcessingState(
        int processingId,
        ProcessingParameters parameters,
        String masterNode,
        List<String> activeNodes,
        List<String> mapCompletedNodes,
        List<String> reduceCompletedNodes,
        Map<String, NodeAssignment> nodeAssignments,
        Function<String, Integer> partitionFunction,
        ProcessingStatus status) {

    static ProcessingState create(int processingId, ProcessingParameters parameters, String masterNode,
            List<String> activeNodes, Function<String, Integer> partitionFunction) {
        var nodeAssignments = new HashMap<String, NodeAssignment>();
        activeNodes.forEach(node -> nodeAssignments.put(node, new NodeAssignment(List.of(), List.of())));

        return new ProcessingState(
                processingId,
                parameters,
                masterNode,
                activeNodes,
                new ArrayList<>(),
                new ArrayList<>(),
                nodeAssignments,
                partitionFunction,
                ProcessingStatus.NOT_STARTED);
    }

    ProcessingState updateStatus(ProcessingStatus status) {
        return new ProcessingState(
                processingId,
                parameters,
                masterNode,
                activeNodes,
                mapCompletedNodes,
                reduceCompletedNodes,
                nodeAssignments,
                partitionFunction,
                status);
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
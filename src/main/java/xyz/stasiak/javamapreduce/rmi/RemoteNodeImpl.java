package xyz.stasiak.javamapreduce.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.Application;

public class RemoteNodeImpl extends UnicastRemoteObject implements RemoteNode {

    private static final Logger LOGGER = Logger.getLogger(RemoteNodeImpl.class.getName());

    private final Map<Integer, ProcessingState> processingStates;

    public RemoteNodeImpl() throws RemoteException {
        super();
        this.processingStates = new ConcurrentHashMap<>();
    }

    public void startProcessing(int processingId, ProcessingParameters parameters) throws RemoteException {
        var activeNodes = Arrays.stream(Application.getProperty("known.nodes").split(","))
                .map(String::trim)
                .toList();
        Function<String, Integer> partitionFunction = (String input) -> input.hashCode() % activeNodes.size();
        var fileAssignments = new HashMap<String, List<String>>();
        activeNodes.forEach(node -> fileAssignments.put(node, List.of()));
        var nodeAddress = Application.getProperty("node.address");
        startNodeProcessing(processingId, parameters, partitionFunction, activeNodes, nodeAddress);
        startMapPhase(processingId, fileAssignments);
    }

    @Override
    public void startNodeProcessing(int processingId, ProcessingParameters parameters, Function<String, Integer> partitionFunction, List<String> activeNodes, String nodeAddress) throws RemoteException {
        LOGGER.info("(%d) Starting node processing".formatted(processingId));
        // TODO: create node directories
        var state = ProcessingState.create(processingId, parameters, nodeAddress, activeNodes, partitionFunction);
        processingStates.put(processingId, state);
    }

    @Override
    public void startMapPhase(int processingId, Map<String, List<String>> fileAssignments) throws RemoteException {
        // TODO set status in Application
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.updateFileAssignments(fileAssignments);
        // TODO start map phase
    }

    @Override
    public void finishMapPhase(int processingId, String node, String masterNode) throws RemoteException {
        // TODO call master
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.mapCompletedNodes().add(node);
        // TODO start reduce phase
    }

    @Override
    public void startReducePhase(int processingId, Map<String, List<Integer>> partitionAssignments)
            throws RemoteException {
        // TODO set status in Application
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.updatePartitionAssignments(partitionAssignments);
        // TODO start reduce phase
    }

    @Override
    public void finishReducePhase(int processingId, String node, String masterNode) throws RemoteException {
        // TODO call master
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.reduceCompletedNodes().add(node);
        // TODO finish processing
    }

    @Override
    public void notifyNodeFailure(int processingId, String failedNode, String masterNode) throws RemoteException {
        var state = processingStates.get(processingId);
        if (state == null) {
            throw new IllegalStateException("Processing %d not found".formatted(processingId));
        }

        state.activeNodes().remove(failedNode);
        state.mapCompletedNodes().remove(failedNode);
        state.reduceCompletedNodes().remove(failedNode);
        state.nodeAssignments().remove(failedNode);
        // TODO redistribute
    }

    @Override
    public void finishProcessing(int processingId) throws RemoteException {
        processingStates.remove(processingId);
    }

    @Override
    public int getProcessingPower() throws RemoteException {
        return Runtime.getRuntime().availableProcessors();
    }

    public ProcessingStatus getProcessingStatus(int processingId) {
        return processingStates.get(processingId).status();
    }
}

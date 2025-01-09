package xyz.stasiak.javamapreduce.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface RemoteNode extends Remote {

    void startNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction,
            List<String> activeNodes, String nodeAddress) throws RemoteException;

    void startMapPhase(int processingId, Map<String, List<String>> fileAssignments) throws RemoteException;

    void finishMapPhase(int processingId, String node, String masterNode) throws RemoteException;

    void startReducePhase(int processingId, Map<String, List<Integer>> partitionAssignments) throws RemoteException;

    void finishReducePhase(int processingId, String node, String masterNode) throws RemoteException;

    void notifyNodeFailure(int processingId, String failedNode, String masterNode) throws RemoteException;

    void finishProcessing(int processingId) throws RemoteException;

    int getProcessingPower() throws RemoteException;

    void isAlive() throws RemoteException;
}

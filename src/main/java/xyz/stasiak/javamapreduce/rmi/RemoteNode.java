package xyz.stasiak.javamapreduce.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.function.Function;

public interface RemoteNode extends Remote {

    void startNodeProcessingAndMapPhase(int processingId, ProcessingParameters parameters, List<String> files,
            Function<String, Integer> partitionFunction,
            String masterNode) throws RemoteException;

    void finishMapPhase(int processingId, String node, int processedFiles) throws RemoteException;

    void startReducePhase(int processingId, List<Integer> partitions) throws RemoteException;

    void finishReducePhase(int processingId, String node, int processedPartitions) throws RemoteException;

    void notifyNodeFailure(int processingId, String failedNode, String masterNode) throws RemoteException;

    void finishProcessing(int processingId) throws RemoteException;

    int getProcessingPower() throws RemoteException;

    void isAlive() throws RemoteException;
}

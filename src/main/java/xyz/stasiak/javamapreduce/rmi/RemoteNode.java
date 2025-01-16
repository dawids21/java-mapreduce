package xyz.stasiak.javamapreduce.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.function.Function;

public interface RemoteNode extends Remote {

    void remoteStartNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction, String masterNode) throws RemoteException;

    void remoteStartMapPhase(int processingId, List<String> files) throws RemoteException;

    void finishMapPhase(int processingId, String node, int processedFiles) throws RemoteException;

    void remoteStartReducePhase(int processingId, List<Integer> partitions) throws RemoteException;

    void finishReducePhase(int processingId, String node, int processedPartitions) throws RemoteException;

    void remoteHandleNodeFailure(int processingId, String failedNode) throws RemoteException;

    void finishProcessing(int processingId) throws RemoteException;

    int getProcessingPower() throws RemoteException;

    void isAlive() throws RemoteException;
}

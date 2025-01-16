package xyz.stasiak.javamapreduce.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.function.Function;

public interface RemoteNode extends Remote {

    void remoteStartNodeProcessing(int processingId, ProcessingParameters parameters,
            Function<String, Integer> partitionFunction, String masterNode) throws RemoteException;

    void remoteStartMapPhase(int processingId, List<String> files) throws RemoteException;

    void remoteFinishMapPhase(int processingId, String node, List<String> processedFiles) throws RemoteException;

    void remoteStartReducePhase(int processingId, List<Integer> partitions) throws RemoteException;

    void remoteFinishReducePhase(int processingId, String node, List<Integer> processedPartitions) throws RemoteException;

    void remoteHandleNodeFailure(int processingId, String failedNode) throws RemoteException;

    void remoteFinishProcessing(int processingId) throws RemoteException;

    int getProcessingPower() throws RemoteException;

    void isAlive() throws RemoteException;
}

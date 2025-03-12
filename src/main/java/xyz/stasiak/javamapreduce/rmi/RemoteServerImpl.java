package xyz.stasiak.javamapreduce.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;

import xyz.stasiak.javamapreduce.util.SystemProperties;

public class RemoteServerImpl extends UnicastRemoteObject implements RemoteServer {

    private final RemoteNodeImpl remoteNode;

    public RemoteServerImpl(RemoteNodeImpl remoteNode) throws RemoteException {
        super(Integer.parseInt(SystemProperties.getRmiPort()) + 2);
        this.remoteNode = remoteNode;
    }

    @Override
    public int startProcessing(ProcessingParameters parameters) throws RemoteException {
        var processingId = new Random().nextInt(1_000_000_000);
        remoteNode.startProcessing(processingId, parameters);
        return processingId;
    }

    @Override
    public ProcessingStatus getProcessingStatus(int processingId) throws RemoteException {
        return remoteNode.getProcessingStatus(processingId);
    }
}

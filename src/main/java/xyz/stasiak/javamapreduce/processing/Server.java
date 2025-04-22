package xyz.stasiak.javamapreduce.processing;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;

import xyz.stasiak.javamapreduce.rmi.RemoteServer;
import xyz.stasiak.javamapreduce.util.SystemProperties;

public class Server extends UnicastRemoteObject implements RemoteServer {

    private final Controller controller;

    public Server(Controller controller) throws RemoteException {
        super(Integer.parseInt(SystemProperties.getRmiPort()) + 2);
        this.controller = controller;
    }

    @Override
    public int startProcessing(ProcessingParameters parameters) throws RemoteException {
        var processingId = new Random().nextInt(1_000_000_000);
        controller.startProcessing(processingId, parameters);
        return processingId;
    }

    @Override
    public ProcessingStatus getProcessingStatus(int processingId) throws RemoteException {
        return controller.getProcessingStatus(processingId);
    }
}

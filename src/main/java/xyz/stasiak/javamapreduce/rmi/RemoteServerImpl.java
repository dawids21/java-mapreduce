package xyz.stasiak.javamapreduce.rmi;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.Application;
import xyz.stasiak.javamapreduce.files.FileManager;

public class RemoteServerImpl extends UnicastRemoteObject implements RemoteServer {

    private static final Logger LOGGER = Logger.getLogger(RemoteServerImpl.class.getSimpleName());
    private final RemoteNodeImpl remoteNode;

    public RemoteServerImpl(RemoteNodeImpl remoteNode) throws RemoteException {
        super();
        this.remoteNode = remoteNode;
    }

    @Override
    public int startProcessing(ProcessingParameters parameters) throws RemoteException {
        var processingId = new Random().nextInt(1_000_000_000);
        try {
            FileManager.createPublicDirectories(processingId, parameters.outputDirectory());
        } catch (IOException e) {
            throw new RemoteException("Failed to create public directories", e);
        }
        LOGGER.info("(%d) [%s] Starting processing with parameters: %s".formatted(processingId,
                this.getClass().getSimpleName(), parameters));
        CompletableFuture.runAsync(() -> {
            try {
                remoteNode.startProcessing(processingId, parameters);
            } catch (RemoteException e) {
                LOGGER.severe("(%d) [%s] Failed to start processing: %s".formatted(
                        processingId, Application.class.getSimpleName(), e.getMessage()));
            }
        });
        return processingId;
    }

    @Override
    public ProcessingStatus getProcessingStatus(int processingId) throws RemoteException {
        LOGGER.info("(%d) [%s] Checking status of processing: %s".formatted(processingId,
                this.getClass().getSimpleName(), processingId));
        var status = remoteNode.getProcessingStatus(processingId);
        LOGGER.info("Processing status: %s".formatted(status));
        return status;
    }
}
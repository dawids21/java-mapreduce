package xyz.stasiak.javamapreduce.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

import xyz.stasiak.javamapreduce.processing.ProcessingParameters;
import xyz.stasiak.javamapreduce.processing.ProcessingStatus;

public interface RemoteServer extends Remote {
    int startProcessing(ProcessingParameters parameters) throws RemoteException;

    ProcessingStatus getProcessingStatus(int processingId) throws RemoteException;
}
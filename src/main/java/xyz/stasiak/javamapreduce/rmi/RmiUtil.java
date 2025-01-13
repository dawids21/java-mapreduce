package xyz.stasiak.javamapreduce.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

class RmiUtil {

    static RemoteNode getRemoteNode(String nodeAddress) throws RemoteNodeUnavailableException {
        try {
            return (RemoteNode) Naming.lookup(nodeAddress);
        } catch (RemoteException | NotBoundException | MalformedURLException e) {
            throw new RemoteNodeUnavailableException(
                    "Node %s is not available: %s".formatted(nodeAddress, e.getMessage()));
        }
    }
}

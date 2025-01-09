package xyz.stasiak.javamapreduce.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

class RmiUtil {

    static RemoteNode getRemoteNode(String nodeAddress) throws RemoteException {
        try {
            var node = (RemoteNode) Naming.lookup(nodeAddress);
            node.isAlive();
            return node;
        } catch (RemoteException | NotBoundException | MalformedURLException e) {
            throw new RemoteException("Node %s is not available: %s".formatted(nodeAddress, e.getMessage()));
        }
    }
}

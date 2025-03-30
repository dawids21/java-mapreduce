package xyz.stasiak.javamapreduce.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class RmiUtil {

    private static final ExecutorService EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    public static void call(String nodeAddress, Consumer<RemoteNode> consumer)
            throws RemoteException, RemoteNodeUnavailableException {
        Future<Void> future = EXECUTOR.submit(() -> {
            try {
                var node = (RemoteNode) Naming.lookup(nodeAddress);
                consumer.accept(node);
                return null;
            } catch (RemoteException | NotBoundException | MalformedURLException e) {
                throw new RemoteNodeUnavailableException(
                        "Node %s is not available: %s".formatted(nodeAddress, e.getMessage()));
            }
        });

        try {
            future.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RemoteNodeUnavailableException(
                    "Connection to node %s timed out after 30 seconds".formatted(nodeAddress));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteNodeUnavailableException(
                    "Connection to node %s was interrupted".formatted(nodeAddress));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RemoteRuntimeException) {
                throw (RemoteException) e.getCause().getCause();
            }
            if (e.getCause() instanceof RemoteNodeUnavailableException) {
                throw (RemoteNodeUnavailableException) e.getCause();
            }
            if (e.getCause() instanceof RemoteException) {
                throw (RemoteException) e.getCause();
            }
            throw new ProcessingException("Processing error: %s".formatted(e.getCause().getMessage()), e.getCause());
        }
    }

    public static <T> T call(String nodeAddress, Function<RemoteNode, T> consumer)
            throws RemoteException, RemoteNodeUnavailableException {
        Future<T> future = EXECUTOR.submit(() -> {
            try {
                var node = (RemoteNode) Naming.lookup(nodeAddress);
                return consumer.apply(node);
            } catch (RemoteException | NotBoundException | MalformedURLException e) {
                throw new RemoteNodeUnavailableException(
                        "Node %s is not available: %s".formatted(nodeAddress, e.getMessage()));
            }
        });

        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RemoteNodeUnavailableException(
                    "Connection to node %s timed out after 10 seconds".formatted(nodeAddress));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteNodeUnavailableException(
                    "Connection to node %s was interrupted".formatted(nodeAddress));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RemoteRuntimeException) {
                throw (RemoteException) e.getCause().getCause();
            }
            if (e.getCause() instanceof RemoteNodeUnavailableException) {
                throw (RemoteNodeUnavailableException) e.getCause();
            }
            if (e.getCause() instanceof RemoteException) {
                throw (RemoteException) e.getCause();
            }
            throw new ProcessingException("Processing error: %s".formatted(e.getCause().getMessage()), e.getCause());
        }
    }

    public static void shutdown() {
        EXECUTOR.shutdownNow();
    }
}

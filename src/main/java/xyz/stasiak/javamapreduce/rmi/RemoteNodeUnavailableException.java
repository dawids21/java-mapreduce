package xyz.stasiak.javamapreduce.rmi;

public class RemoteNodeUnavailableException extends Exception {

    public RemoteNodeUnavailableException(String message) {
        super(message);
    }

    public RemoteNodeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

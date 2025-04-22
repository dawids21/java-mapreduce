package xyz.stasiak.javamapreduce.processing;

public class ProcessingCancelledException extends ProcessingException {
    public ProcessingCancelledException(int processingId, String message) {
        super("Processing %d was cancelled: %s".formatted(processingId, message));
    }
}

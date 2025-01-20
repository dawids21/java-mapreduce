package xyz.stasiak.javamapreduce.rmi;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public record CancellationToken(AtomicBoolean cancelled) implements Serializable {

    static CancellationToken create() {
        return new CancellationToken(new AtomicBoolean(false));
    }

    void cancel() {
        cancelled.set(true);
    }

    boolean isCancelled() {
        return cancelled.get();
    }

    public void throwIfCancelled(int processingId, String message) {
        if (isCancelled()) {
            throw new ProcessingCancelledException(processingId, message);
        }
    }
}
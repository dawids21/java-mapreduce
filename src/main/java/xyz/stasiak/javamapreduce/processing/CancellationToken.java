package xyz.stasiak.javamapreduce.processing;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public record CancellationToken(AtomicBoolean cancelled) implements Serializable {

    static CancellationToken create() {
        return new CancellationToken(new AtomicBoolean(false));
    }

    void cancel() {
        cancelled.set(true);
    }

    public void throwIfCancelled(int processingId, String message) {
        if (cancelled.get()) {
            throw new ProcessingCancelledException(processingId, message);
        }
    }
}
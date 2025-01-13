package xyz.stasiak.javamapreduce.map;

public record MapPartitionResult(Throwable error) {

    static MapPartitionResult success() {
        return new MapPartitionResult(null);
    }

    static MapPartitionResult failure(Throwable error) {
        return new MapPartitionResult(error);
    }

    boolean isSuccess() {
        return error == null;
    }
}


package xyz.stasiak.javamapreduce.map;

public record MapPartitionResult(String error) {

    static MapPartitionResult success() {
        return new MapPartitionResult(null);
    }

    static MapPartitionResult failure(String error) {
        return new MapPartitionResult(error);
    }

    boolean isSuccess() {
        return error == null;
    }
}


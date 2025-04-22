package xyz.stasiak.javamapreduce.map;

import java.io.Serializable;
import java.util.function.Function;

public record PartitionFunction(int partitionCount) implements Function<String, Integer>, Serializable {

    public PartitionFunction {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be positive");
        }
    }

    @Override
    public Integer apply(String key) {
        if (key == null) {
            return 0;
        }

        int hash = key.hashCode();
        hash = hash & Integer.MAX_VALUE;
        return hash % partitionCount;
    }
}
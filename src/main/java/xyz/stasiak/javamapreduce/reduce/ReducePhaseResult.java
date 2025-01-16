package xyz.stasiak.javamapreduce.reduce;

import java.util.List;

public record ReducePhaseResult(List<Integer> processedPartitions) {
}

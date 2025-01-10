package xyz.stasiak.javamapreduce.rmi;

import java.util.function.Function;

record ProcessingInfo(
        int processingId,
        ProcessingParameters parameters,
        String masterNode,
        Function<String, Integer> partitionFunction) {
}

package xyz.stasiak.javamapreduce.processing;

import java.util.function.Function;

record ProcessingInfo(
                int processingId,
                ProcessingParameters parameters,
                String masterNode,
                Function<String, Integer> partitionFunction,
                CancellationToken cancellationToken) {

        static ProcessingInfo create(int processingId, ProcessingParameters parameters, String masterNode,
                        Function<String, Integer> partitionFunction) {
                return new ProcessingInfo(processingId, parameters, masterNode, partitionFunction,
                                CancellationToken.create());
        }
}

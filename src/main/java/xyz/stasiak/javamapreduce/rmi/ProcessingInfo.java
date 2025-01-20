package xyz.stasiak.javamapreduce.rmi;

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

        void cancel() {
                cancellationToken.cancel();
        }

        boolean isCancelled() {
                return cancellationToken.isCancelled();
        }

        void throwIfCancelled() {
                if (isCancelled()) {
                        throw new ProcessingCancelledException(processingId, "Processing cancelled");
                }
        }
}

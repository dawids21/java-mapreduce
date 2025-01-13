package xyz.stasiak.javamapreduce.reduce;

record MergeReduceResult(Throwable error) {
    static MergeReduceResult success() {
        return new MergeReduceResult(null);
    }

    static MergeReduceResult failure(Throwable error) {
        return new MergeReduceResult(error);
    }

    boolean isSuccess() {
        return error == null;
    }
}
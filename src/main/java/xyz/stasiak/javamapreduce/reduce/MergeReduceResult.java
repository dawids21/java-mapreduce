package xyz.stasiak.javamapreduce.reduce;

record MergeReduceResult(String error) {
    static MergeReduceResult success() {
        return new MergeReduceResult(null);
    }

    static MergeReduceResult failure(String error) {
        return new MergeReduceResult(error);
    }

    boolean isSuccess() {
        return error == null;
    }
}
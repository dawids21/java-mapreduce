package xyz.stasiak.javamapreduce;

public enum ProcessingStatus {
    NOT_STARTED,
    MAPPING,
    MAPPING_FAILED,
    REDUCING,
    REDUCING_FAILED,
    FINISHED
}

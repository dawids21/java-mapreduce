package xyz.stasiak.javamapreduce.processing;

public enum ProcessingStatus {
    NOT_FOUND,
    NOT_STARTED,
    MAPPING,
    MAPPING_FAILED,
    REDUCING,
    REDUCING_FAILED,
    FINISHED
}

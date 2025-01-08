package xyz.stasiak.javamapreduce.rmi;

public enum ProcessingStatus {
    NOT_STARTED,
    MAPPING,
    MAPPING_FAILED,
    REDUCING,
    REDUCING_FAILED,
    FINISHED
}

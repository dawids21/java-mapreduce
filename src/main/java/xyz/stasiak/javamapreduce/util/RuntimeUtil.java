package xyz.stasiak.javamapreduce.util;

public class RuntimeUtil {
    public static int getProcessingPower() {
        return Runtime.getRuntime().availableProcessors();
    }
}

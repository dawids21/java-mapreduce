package xyz.stasiak.javamapreduce.util;

public class RuntimeUtil {
    public static int getProcessingPower(double multiplier) {
        return (int) (Runtime.getRuntime().availableProcessors() * multiplier);
    }
}

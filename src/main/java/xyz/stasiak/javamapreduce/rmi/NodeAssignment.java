package xyz.stasiak.javamapreduce.rmi;

import java.io.Serializable;
import java.util.List;

public record NodeAssignment(
        List<String> files,
        List<Integer> partitions) implements Serializable {
}

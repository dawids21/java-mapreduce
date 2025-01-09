package xyz.stasiak.javamapreduce.rmi;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

public record NodeAssignment(
        List<Path> files,
        List<Integer> partitions) implements Serializable {
}
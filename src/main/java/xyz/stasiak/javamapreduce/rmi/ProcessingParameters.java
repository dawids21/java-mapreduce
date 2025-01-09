package xyz.stasiak.javamapreduce.rmi;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

public record ProcessingParameters(
        String inputDirectory,
        String outputDirectory,
        String mapperClassName,
        String reducerClassName) implements Serializable {

    public static ProcessingParameters fromArguments(List<String> arguments) {
        if (arguments.size() != 4) {
            throw new IllegalArgumentException("Processing parameters require exactly 4 arguments");
        }
        return new ProcessingParameters(
                Path.of(arguments.get(0)).toAbsolutePath().toString(),
                Path.of(arguments.get(1)).toAbsolutePath().toString(),
                arguments.get(2),
                arguments.get(3));
    }
}

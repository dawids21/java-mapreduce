package xyz.stasiak.javamapreduce;

import java.nio.file.Path;
import java.util.List;

public record ProcessingParameters(
        Path inputDirectory,
        Path outputDirectory,
        String mapperClassName,
        String reducerClassName) {

    public static ProcessingParameters fromArguments(List<String> arguments) {
        if (arguments.size() != 4) {
            throw new IllegalArgumentException("Processing parameters require exactly 4 arguments");
        }
        return new ProcessingParameters(
                Path.of(arguments.get(0)).toAbsolutePath(),
                Path.of(arguments.get(1)).toAbsolutePath(),
                arguments.get(2),
                arguments.get(3));
    }
}

package xyz.stasiak.javamapreduce.processing;

import java.io.Serializable;
import java.util.List;

import xyz.stasiak.javamapreduce.util.FilesUtil;

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
                FilesUtil.getFilesDirectory(arguments.get(0)).toString(),
                FilesUtil.getFilesDirectory(arguments.get(1)).toString(),
                arguments.get(2),
                arguments.get(3));
    }
}

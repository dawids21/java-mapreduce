package xyz.stasiak.javamapreduce.cli;

import java.util.List;

import xyz.stasiak.javamapreduce.rmi.ProcessingParameters;

public record CommandWithArguments(Command command, List<String> arguments, String rawCommand) {
    public ProcessingParameters toProcessingParameters() {
        if (command != Command.START) {
            throw new IllegalStateException("Processing parameters are only valid for START command");
        }
        return ProcessingParameters.fromArguments(arguments);
    }
}

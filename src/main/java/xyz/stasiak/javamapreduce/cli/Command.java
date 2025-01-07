package xyz.stasiak.javamapreduce.cli;

public enum Command {
    START,
    STATUS,
    EXIT;

    public static Command fromString(String input) {
        try {
            return Command.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
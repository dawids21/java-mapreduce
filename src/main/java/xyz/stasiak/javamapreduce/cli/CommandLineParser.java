package xyz.stasiak.javamapreduce.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class CommandLineParser {

    private static final Logger LOGGER = Logger.getLogger(CommandLineParser.class.getName());

    public CommandLineParser() {
    }

    public CommandWithArguments parse(String line) {
        var parts = parseLine(line);
        if (parts.isEmpty()) {
            return null;
        }

        var command = Command.fromString(parts.get(0));
        if (command == null) {
            return null;
        }

        var arguments = parts.subList(1, parts.size());
        if (!validateArguments(command, arguments)) {
            LOGGER.warning("Invalid number of arguments for command: " + command);
            return null;
        }

        return new CommandWithArguments(command, arguments, line);
    }

    private boolean validateArguments(Command command, List<String> arguments) {
        return switch (command) {
            case START -> arguments.size() == 4;
            case STATUS -> arguments.size() == 1;
            case EXIT -> arguments.isEmpty();
        };
    }

    private List<String> parseLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return List.of();
        }

        var result = new ArrayList<String>();
        var current = new StringBuilder();
        var inQuotes = false;

        for (char c : line.trim().toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ' ' && !inQuotes) {
                if (current.length() > 0) {
                    result.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            result.add(current.toString());
        }

        if (inQuotes) {
            LOGGER.warning("Unclosed quotes in command");
            return List.of();
        }

        return result;
    }
}

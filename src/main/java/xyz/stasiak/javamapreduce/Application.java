package xyz.stasiak.javamapreduce;

import xyz.stasiak.javamapreduce.cli.CommandLineParser;
import xyz.stasiak.javamapreduce.cli.CommandWithArguments;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Application {

    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());

    static {
        try {
            LogManager.getLogManager().readConfiguration(
                    Application.class.getClassLoader().getResourceAsStream("logging.properties"));
        } catch (IOException e) {
            System.err.println("Could not load logging configuration");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Starting Java MapReduce application");
        var parser = new CommandLineParser();
        try (var scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                var line = scanner.nextLine().trim();

                if (line.isEmpty()) {
                    continue;
                }

                var commandWithArguments = parser.parse(line);

                if (commandWithArguments == null) {
                    LOGGER.severe("Unknown command: " + line);
                    continue;
                }

                switch (commandWithArguments.command()) {
                    case START -> handleStart(commandWithArguments);
                    case STATUS -> handleStatus(commandWithArguments);
                    case EXIT -> {
                        LOGGER.info("Shutting down");
                        return;
                    }
                }
            }
        }
    }
    
    private static void handleStart(CommandWithArguments command) {
        var parameters = command.toProcessingParameters();
        var processingId = new Random().nextInt(1_000_000_000);
        LOGGER.info("(%d) [%s] Starting processing with parameters: %s".formatted(processingId, Application.class.getSimpleName(), parameters));
    }

    private static void handleStatus(CommandWithArguments command) {
        var processingIdStr = command.arguments().get(0);
        var processingId = Integer.parseInt(processingIdStr);
        LOGGER.info("(%d) [%s] Checking status of processing: %s".formatted(processingId, Application.class.getSimpleName(), processingId));
        LOGGER.info("Processing status: %s".formatted(ProcessingStatus.NOT_STARTED)); //TODO
    }
}

package xyz.stasiak.javamapreduce;

import xyz.stasiak.javamapreduce.cli.CommandLineParser;
import xyz.stasiak.javamapreduce.cli.CommandWithArguments;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.LogManager;
import java.util.logging.Logger;

class Application {
    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());
    private final CommandLineParser parser;
    private final Random random;

    static {
        try {
            LogManager.getLogManager().readConfiguration(
                    Application.class.getClassLoader().getResourceAsStream("logging.properties"));
        } catch (IOException e) {
            System.err.println("Could not load logging configuration");
            e.printStackTrace();
        }
    }

    Application() {
        this.parser = new CommandLineParser();
        this.random = new Random();
    }

    public static void main(String[] args) {
        var application = new Application();
        application.run();
    }

    void run() {
        LOGGER.info("Starting Java MapReduce application");
        try (var scanner = new Scanner(System.in)) {
            processCommands(scanner);
        }
    }

    private void processCommands(Scanner scanner) {
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

            if (processCommand(commandWithArguments)) {
                return;
            }
        }
    }

    private boolean processCommand(CommandWithArguments commandWithArguments) {
        return switch (commandWithArguments.command()) {
            case START -> {
                handleStart(commandWithArguments);
                yield false;
            }
            case STATUS -> {
                handleStatus(commandWithArguments);
                yield false;
            }
            case EXIT -> {
                LOGGER.info("Shutting down");
                yield true;
            }
        };
    }

    int handleStart(CommandWithArguments command) {
        var parameters = command.toProcessingParameters();
        var processingId = random.nextInt(1_000_000_000);
        LOGGER.info("(%d) [%s] Starting processing with parameters: %s".formatted(processingId,
                Application.class.getSimpleName(), parameters));
        return processingId;
    }

    ProcessingStatus handleStatus(CommandWithArguments command) {
        var processingIdStr = command.arguments().get(0);
        var processingId = Integer.parseInt(processingIdStr);
        LOGGER.info("(%d) [%s] Checking status of processing: %s".formatted(processingId,
                Application.class.getSimpleName(), processingId));
        LOGGER.info("Processing status: %s".formatted(ProcessingStatus.FINISHED)); // TODO
        return ProcessingStatus.FINISHED;
    }
}

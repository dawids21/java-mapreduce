package xyz.stasiak.javamapreduce;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.cli.CommandLineParser;
import xyz.stasiak.javamapreduce.cli.CommandWithArguments;
import xyz.stasiak.javamapreduce.rmi.RemoteServer;

public class CliApplication {
    private static final Logger LOGGER = Logger.getLogger(CliApplication.class.getName());
    private static final Properties properties = new Properties();
    private final CommandLineParser parser;
    private Registry rmiRegistry;
    private RemoteServer remoteServer;

    static {
        try {
            LogManager.getLogManager().readConfiguration(
                    CliApplication.class.getClassLoader().getResourceAsStream("logging.properties"));
            loadProperties();
        } catch (IOException e) {
            System.err.println("Could not load configuration");
            e.printStackTrace();
        }
    }

    public CliApplication() {
        this.parser = new CommandLineParser();
        try {
            connectToServer();
        } catch (RemoteException | NotBoundException e) {
            LOGGER.severe("Failed to connect to server: " + e.getMessage());
            throw new IllegalStateException("Could not connect to server", e);
        }
    }

    private static void loadProperties() throws IOException {
        try (var input = CliApplication.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IllegalStateException("Unable to find application.properties");
            }
            properties.load(input);
        }
    }

    public static void main(String[] args) {
        var application = new CliApplication();
        application.run();
    }

    void run() {
        LOGGER.info("Starting Java MapReduce CLI application");
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

            try {
                if (processCommand(commandWithArguments)) {
                    return;
                }
            } catch (IOException e) {
                LOGGER.severe("Failed to process command: " + e.getMessage());
            }
        }
    }

    private boolean processCommand(CommandWithArguments commandWithArguments) throws IOException {
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

    private void connectToServer() throws RemoteException, NotBoundException {
        var port = Integer.parseInt(properties.getProperty("rmi.port", "1099"));
        rmiRegistry = LocateRegistry.getRegistry(port);
        remoteServer = (RemoteServer) rmiRegistry.lookup("server");
        LOGGER.info("Connected to RMI server on port " + port);
    }

    private void handleStart(CommandWithArguments command) throws IOException {
        var parameters = command.toProcessingParameters();
        try {
            var processingId = remoteServer.startProcessing(parameters);
            LOGGER.info("Processing started with ID: %d".formatted(processingId));
        } catch (RemoteException e) {
            LOGGER.severe("Failed to start processing: " + e.getMessage());
            throw new IOException("Failed to start processing", e);
        }
    }

    private void handleStatus(CommandWithArguments command) throws IOException {
        var processingId = Integer.parseInt(command.arguments().get(0));
        try {
            var status = remoteServer.getProcessingStatus(processingId);
            LOGGER.info("Processing %d status: %s".formatted(processingId, status));
        } catch (RemoteException e) {
            LOGGER.severe("Failed to get processing status: " + e.getMessage());
            throw new IOException("Failed to get processing status", e);
        }
    }
}
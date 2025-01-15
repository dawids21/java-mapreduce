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
import xyz.stasiak.javamapreduce.util.LoggingUtil;

public class CliApplication {
    private static final Logger LOGGER = Logger.getLogger(CliApplication.class.getName());
    private static final Properties properties = new Properties();
    private final CommandLineParser parser = new CommandLineParser();
    private Registry rmiRegistry;
    private RemoteServer remoteServer;

    static {
        try (var loggingProperties = CliApplication.class.getClassLoader().getResourceAsStream("logging.properties");
                var applicationProperties = CliApplication.class.getClassLoader()
                        .getResourceAsStream("application.properties")) {
            LogManager.getLogManager().readConfiguration(loggingProperties);
            properties.load(applicationProperties);
        } catch (IOException e) {
            LoggingUtil.logSevere(LOGGER, CliApplication.class, "Could not load configuration", e);
            throw new IllegalStateException("Could not load configuration", e);
        }
    }

    public CliApplication() {
        try {
            connectToServer();
        } catch (RemoteException | NotBoundException e) {
            LoggingUtil.logSevere(LOGGER, CliApplication.class, "Failed to connect to server", e);
            throw new IllegalStateException("Could not connect to server", e);
        }
    }

    private void connectToServer() throws RemoteException, NotBoundException {
        var port = Integer.parseInt(properties.getProperty("rmi.port", "1099"));
        rmiRegistry = LocateRegistry.getRegistry(port);
        remoteServer = (RemoteServer) rmiRegistry.lookup("server");
        LoggingUtil.logInfo(LOGGER, CliApplication.class, "Connected to RMI server on port " + port);
    }

    public static void main(String[] args) {
        var application = new CliApplication();
        application.run();
    }

    void run() {
        LoggingUtil.logInfo(LOGGER, CliApplication.class, "Starting Java MapReduce CLI application");
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
                System.out.println("Unknown command: " + line);
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
                System.out.println("Shutting down");
                yield true;
            }
        };
    }

    private void handleStart(CommandWithArguments command) {
        System.out.println("Starting processing");
        var parameters = command.toProcessingParameters();
        try {
            var processingId = remoteServer.startProcessing(parameters);
            System.out.printf("Processing started with ID: %d%n", processingId);
        } catch (RemoteException e) {
            System.out.println("Failed to start processing");
        }
    }

    private void handleStatus(CommandWithArguments command) {
        var processingId = Integer.parseInt(command.arguments().getFirst());
        System.out.println("Checking processing status");
        try {
            var status = remoteServer.getProcessingStatus(processingId);
            System.out.printf("Processing %d status: %s%n", processingId, status);
        } catch (RemoteException e) {
            System.out.println("Failed to get processing status");
        }
    }
}
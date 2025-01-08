package xyz.stasiak.javamapreduce;

import xyz.stasiak.javamapreduce.cli.CommandLineParser;
import xyz.stasiak.javamapreduce.cli.CommandWithArguments;
import xyz.stasiak.javamapreduce.files.FileManager;
import xyz.stasiak.javamapreduce.rmi.ProcessingStatus;
import xyz.stasiak.javamapreduce.rmi.RemoteNodeImpl;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;

public class Application {
    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());
    private static final Properties properties = new Properties();
    private final CommandLineParser parser;
    private Registry rmiRegistry;
    private RemoteNodeImpl remoteNode;

    static {
        try {
            LogManager.getLogManager().readConfiguration(
                    Application.class.getClassLoader().getResourceAsStream("logging.properties"));
            loadProperties();
        } catch (IOException e) {
            System.err.println("Could not load configuration");
            e.printStackTrace();
        }
    }

    private static void loadProperties() throws IOException {
        try (var input = Application.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IllegalStateException("Unable to find application.properties");
            }
            properties.load(input);
        }
    }

    public static Properties getProperties() {
        return properties;
    }

    Application() {
        this.parser = new CommandLineParser();
        initializeRmiRegistry();
        initializeRemoteNode();
        registerShutdownHook();
    }

    private void initializeRmiRegistry() {
        try {
            var rmiPort = Integer.parseInt(getProperty("rmi.port"));
            rmiRegistry = LocateRegistry.createRegistry(rmiPort);
            LOGGER.info("RMI Registry started on port " + rmiPort);
        } catch (RemoteException e) {
            try {
                LOGGER.info("Registry already exists, attempting to locate it");
                rmiRegistry = LocateRegistry.getRegistry();
                rmiRegistry.list();
                LOGGER.info("Successfully connected to existing RMI Registry");
            } catch (RemoteException re) {
                LOGGER.severe("Failed to create or locate RMI registry: " + re.getMessage());
                throw new IllegalStateException("Could not initialize RMI registry", re);
            }
        }
    }

    private void initializeRemoteNode() {
        try {
            remoteNode = new RemoteNodeImpl();
            rmiRegistry.rebind("node", remoteNode);
        } catch (RemoteException e) {
            LOGGER.severe("Failed to initialize RemoteNode: " + e.getMessage());
            throw new IllegalStateException("Could not initialize RemoteNode", e);
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (remoteNode != null) {
                    rmiRegistry.unbind("node");
                    LOGGER.info("RemoteNode unbound from registry");
                }
            } catch (Exception e) {
                LOGGER.warning("Error while cleaning up RemoteNode: " + e.getMessage());
            }
        }));
    }

    public static String getProperty(String key) {
        return System.getProperty(key, properties.getProperty(key));
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
                System.exit(0);
                yield true;
            }
        };
    }

    int handleStart(CommandWithArguments command) throws IOException {
        var parameters = command.toProcessingParameters();
        var processingId = new Random().nextInt(1_000_000_000);
        FileManager.createPublicDirectories(processingId, parameters.outputDirectory());
        LOGGER.info("(%d) [%s] Starting processing with parameters: %s".formatted(processingId,
                Application.class.getSimpleName(), parameters));
        try {
            remoteNode.startProcessing(processingId, parameters);
        } catch (RemoteException e) {
            LOGGER.severe("Failed to start processing: " + e.getMessage());
            throw new IOException("Failed to start processing", e);
        }
        return processingId;
    }

    ProcessingStatus handleStatus(CommandWithArguments command) {
        var processingIdStr = command.arguments().get(0);
        var processingId = Integer.parseInt(processingIdStr);
        LOGGER.info("(%d) [%s] Checking status of processing: %s".formatted(processingId,
                Application.class.getSimpleName(), processingId));
        var status = remoteNode.getProcessingStatus(processingId);
        LOGGER.info("Processing status: %s".formatted(status));
        return status;
    }
}

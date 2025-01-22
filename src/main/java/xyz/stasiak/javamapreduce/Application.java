package xyz.stasiak.javamapreduce;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import xyz.stasiak.javamapreduce.rmi.RemoteNodeImpl;
import xyz.stasiak.javamapreduce.rmi.RemoteServerImpl;
import xyz.stasiak.javamapreduce.util.LoggingUtil;

public class Application {
    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());
    private static final Properties properties = new Properties();
    private Registry rmiRegistry;
    private RemoteNodeImpl remoteNode;
    private RemoteServerImpl remoteServer;

    static {
        try (var loggingProperties = Application.class.getClassLoader().getResourceAsStream("logging.properties");
                var applicationProperties = Application.class.getClassLoader()
                        .getResourceAsStream("application.properties")) {
            LogManager.getLogManager().readConfiguration(loggingProperties);
            properties.load(applicationProperties);
        } catch (IOException e) {
            LoggingUtil.logSevere(LOGGER, Application.class, "Could not load configuration", e);
            throw new IllegalStateException("Could not load configuration", e);
        }
    }

    Application() {
        LoggingUtil.logInfo(LOGGER, Application.class, "Starting Java MapReduce application");
        initializeRmiRegistry();
        initializeRemoteNode();
        registerShutdownHook();
    }

    private void initializeRmiRegistry() {
        try {
            var rmiPort = Integer.parseInt(getProperty("rmi.port"));
            rmiRegistry = LocateRegistry.createRegistry(rmiPort);
            LoggingUtil.logInfo(LOGGER, Application.class, "RMI Registry started on port " + rmiPort);
        } catch (RemoteException e) {
            try {
                LoggingUtil.logWarning(LOGGER, Application.class, "Registry already exists, attempting to locate it",
                        e);
                rmiRegistry = LocateRegistry.getRegistry();
                rmiRegistry.list();
                LoggingUtil.logInfo(LOGGER, Application.class, "Successfully connected to existing RMI Registry");
            } catch (RemoteException re) {
                LoggingUtil.logSevere(LOGGER, Application.class, "Failed to create or locate RMI registry", re);
                throw new IllegalStateException("Failed to create or locate RMI registry", re);
            }
        }
    }

    private void initializeRemoteNode() {
        try {
            remoteNode = new RemoteNodeImpl();
            rmiRegistry.rebind("node", remoteNode);
            remoteServer = new RemoteServerImpl(remoteNode);
            rmiRegistry.rebind("server", remoteServer);
        } catch (RemoteException e) {
            LoggingUtil.logSevere(LOGGER, Application.class, "Failed to initialize RemoteNode", e);
            throw new IllegalStateException("Could not initialize RemoteNode", e);
        }
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (remoteNode != null) {
                    remoteNode.shutdownExecutor();
                    rmiRegistry.unbind("node");
                    LoggingUtil.logInfo(LOGGER, Application.class, "RemoteNode unbound from registry");
                }
                if (remoteServer != null) {
                    rmiRegistry.unbind("server");
                    LoggingUtil.logInfo(LOGGER, Application.class, "RemoteServer unbound from registry");
                }
            } catch (Exception e) {
                LoggingUtil.logWarning(LOGGER, Application.class, "Error while cleaning up RemoteNode", e);
            }
        }));
    }

    public static void main(String[] args) {
        new Application();
    }

    public static String getNodeDirectory() {
        return getProperty("node.directory");
    }

    public static String getPublicDirectory() {
        return getProperty("public.directory");
    }

    public static String getRmiPort() {
        return getProperty("rmi.port");
    }

    public static String getNodeAddress() {
        return getProperty("node.address");
    }

    public static List<String> getKnownNodes() {
        return Arrays.stream(getProperty("known.nodes").split(","))
                .map(String::trim)
                .toList();
    }

    public RemoteServerImpl getRemoteServer() {
        return remoteServer;
    }

    private static String getProperty(String key) {
        var property = System.getProperty(key, properties.getProperty(key));
        if (property == null) {
            throw new IllegalStateException("Property %s not set in application.properties".formatted(key));
        }
        return property;
    }
}


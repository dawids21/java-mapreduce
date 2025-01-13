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

public class Application {
    private static final Logger LOGGER = Logger.getLogger(Application.class.getName());
    private static final Properties properties = new Properties();
    private Registry rmiRegistry;
    private RemoteNodeImpl remoteNode;
    private RemoteServerImpl remoteServer;

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
        LOGGER.info("Starting Java MapReduce application");
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
            remoteServer = new RemoteServerImpl(remoteNode);
            rmiRegistry.rebind("server", remoteServer);
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
                if (remoteServer != null) {
                    rmiRegistry.unbind("server");
                    LOGGER.info("RemoteServer unbound from registry");
                }
            } catch (Exception e) {
                LOGGER.warning("Error while cleaning up RemoteNode: " + e.getMessage());
            }
        }));
    }

    public static void main(String[] args) {
        new Application();
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
        return System.getProperty(key, properties.getProperty(key));
    }
}

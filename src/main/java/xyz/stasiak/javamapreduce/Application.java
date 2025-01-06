package xyz.stasiak.javamapreduce;

import java.io.IOException;
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
    }
}
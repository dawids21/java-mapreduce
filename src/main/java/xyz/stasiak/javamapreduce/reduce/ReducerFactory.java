package xyz.stasiak.javamapreduce.reduce;

import java.util.logging.Logger;

public class ReducerFactory {

    private static final Logger LOGGER = Logger.getLogger(ReducerFactory.class.getName());

    public static Reducer createReducer(String reducerClassName) {
        try {
            Class<?> reducerClass = Class.forName(reducerClassName);
            if (!Reducer.class.isAssignableFrom(reducerClass)) {
                throw new RuntimeException(
                        "Class %s does not implement Reducer interface".formatted(reducerClassName));
            }
            return (Reducer) reducerClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            LOGGER.severe("Failed to create reducer: " + e.getMessage());
            throw new RuntimeException("Failed to create reducer", e);
        }
    }
}
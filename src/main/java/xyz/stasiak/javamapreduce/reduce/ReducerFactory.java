package xyz.stasiak.javamapreduce.reduce;

public class ReducerFactory {

    public static Reducer createReducer(String reducerClassName) {
        try {
            Class<?> reducerClass = Class.forName(reducerClassName);
            if (!Reducer.class.isAssignableFrom(reducerClass)) {
                throw new RuntimeException(
                        "Class %s does not implement Reducer interface".formatted(reducerClassName));
            }
            return (Reducer) reducerClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create reducer", e);
        }
    }
}
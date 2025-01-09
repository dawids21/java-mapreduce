package xyz.stasiak.javamapreduce.map;

import java.util.logging.Logger;

public class MapperFactory {

    private static final Logger LOGGER = Logger.getLogger(MapperFactory.class.getName());

    @SuppressWarnings("unchecked")
	public static <K, V> Mapper<K, V> createMapper(String mapperClassName) {
        try {
            Class<?> mapperClass = Class.forName(mapperClassName);
            if (!Mapper.class.isAssignableFrom(mapperClass)) {
                throw new RuntimeException(
                        "Class %s does not implement Mapper interface".formatted(mapperClassName));
            }
            return (Mapper<K, V>) mapperClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            LOGGER.severe("Failed to create mapper: " + e.getMessage());
            throw new RuntimeException("Failed to create mapper: " + e.getMessage(), e);
        }
    }
}
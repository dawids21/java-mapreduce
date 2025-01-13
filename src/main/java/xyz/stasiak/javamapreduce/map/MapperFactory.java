package xyz.stasiak.javamapreduce.map;

public class MapperFactory {

	public static Mapper createMapper(String mapperClassName) {
        try {
            Class<?> mapperClass = Class.forName(mapperClassName);
            if (!Mapper.class.isAssignableFrom(mapperClass)) {
                throw new RuntimeException(
                        "Class %s does not implement Mapper interface".formatted(mapperClassName));
            }
            return (Mapper) mapperClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create mapper", e);
        }
    }
}
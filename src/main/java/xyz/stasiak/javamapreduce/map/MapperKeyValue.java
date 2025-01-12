package xyz.stasiak.javamapreduce.map;

import java.io.Serializable;
import java.util.Objects;

public record MapperKeyValue(String key, String value) implements Serializable {
    public MapperKeyValue {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }
}
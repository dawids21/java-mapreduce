package xyz.stasiak.javamapreduce.util;

import java.io.Serializable;
import java.util.Objects;

public record KeyValue(String key, String value) implements Comparable<KeyValue>, Serializable {
    public KeyValue {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }

    @Override
    public int compareTo(KeyValue other) {
        return this.key.compareTo(other.key);
    }
}
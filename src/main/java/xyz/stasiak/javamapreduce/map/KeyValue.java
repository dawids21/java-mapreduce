package xyz.stasiak.javamapreduce.map;

import java.io.Serializable;
import java.util.Objects;

public record KeyValue<K, V>(K key, V value) implements Serializable {
    public KeyValue {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
    }
}
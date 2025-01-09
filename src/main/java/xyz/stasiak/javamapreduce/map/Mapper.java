package xyz.stasiak.javamapreduce.map;

import java.io.IOException;
import java.util.List;

public interface Mapper<K, V> {
    List<KeyValue<K, V>> map(String line) throws IOException;
}
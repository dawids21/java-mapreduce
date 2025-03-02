package xyz.stasiak.javamapreduce.map;

import xyz.stasiak.javamapreduce.util.KeyValue;

import java.util.List;

public interface Mapper {
    List<KeyValue> map(String line);
}

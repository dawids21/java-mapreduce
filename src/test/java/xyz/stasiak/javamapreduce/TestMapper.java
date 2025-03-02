package xyz.stasiak.javamapreduce;

import java.util.Arrays;
import java.util.List;

import xyz.stasiak.javamapreduce.map.Mapper;
import xyz.stasiak.javamapreduce.util.KeyValue;

public class TestMapper implements Mapper {

    @Override
    public List<KeyValue> map(String input) {
        return Arrays.stream(input.split(" "))
                .map(word -> new KeyValue(word, "1"))
                .toList();
    }
}

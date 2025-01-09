package xyz.stasiak.javamapreduce.test;

import java.util.Arrays;
import java.util.List;

import xyz.stasiak.javamapreduce.map.KeyValue;
import xyz.stasiak.javamapreduce.map.Mapper;

public class ExampleMapper implements Mapper<String, Integer> {

    @Override
    public List<KeyValue<String, Integer>> map(String input) {
        return Arrays.stream(input.split(" "))
                .map(word -> new KeyValue<>(word, 1))
                .toList();
    }
}

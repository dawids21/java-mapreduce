package xyz.stasiak.javamapreduce.test;

import java.util.Arrays;
import java.util.List;

import xyz.stasiak.javamapreduce.map.Mapper;
import xyz.stasiak.javamapreduce.map.MapperKeyValue;

public class ExampleMapper implements Mapper {

    @Override
    public List<MapperKeyValue> map(String input) {
        return Arrays.stream(input.split(" "))
                .map(word -> new MapperKeyValue(word, "1"))
                .toList();
    }
}

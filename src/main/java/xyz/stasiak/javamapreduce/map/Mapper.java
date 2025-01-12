package xyz.stasiak.javamapreduce.map;

import java.util.List;

public interface Mapper {
    List<MapperKeyValue> map(String line);
}

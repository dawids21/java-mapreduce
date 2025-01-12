package xyz.stasiak.javamapreduce.reduce;

import java.util.List;

public interface Reducer {
    String reduce(String key, List<String> values);
}

package cis5550.generic;

import cis5550.kvs.Row;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryTableInfo implements TableInfo {
    private final Map<String, Row> data = new ConcurrentHashMap<>();

    public Map<String, Row> getData() {
        return data;
    }

    @Override
    public Set<String> getRowKeys() {
        return data.keySet();
    }

    @Override
    public int getRowCount() {
        return data.size();
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public boolean isCompactionNeeded() {
        return false;
    }
}

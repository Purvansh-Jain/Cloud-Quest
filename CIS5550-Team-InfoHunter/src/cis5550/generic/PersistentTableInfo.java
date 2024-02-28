package cis5550.generic;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentTableInfo implements TableInfo {
    private Map<String, Long> index = new ConcurrentHashMap<>();

    private boolean isDirty = false;

    public Map<String, Long> getIndex() {
        return index;
    }

    public void setIndex(Map<String, Long> index) {
        this.index = index;
    }

    @Override
    public Set<String> getRowKeys() {
        return index.keySet();
    }

    @Override
    public int getRowCount() {
        return index.size();
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public boolean isCompactionNeeded() {
        return isDirty;
    }

    public void setIsDirty(boolean isDirty) {
        this.isDirty = isDirty;
    }
}

package cis5550.generic;

import java.util.Set;

public interface TableInfo {
    Set<String> getRowKeys();

    int getRowCount();

    boolean isPersistent();

    boolean isCompactionNeeded();
}

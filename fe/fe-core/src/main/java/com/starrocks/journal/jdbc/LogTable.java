// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Config.java
package com.starrocks.journal.jdbc;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.regex.Pattern;

public class LogTable implements Comparable<LogTable> {

    private final long tableId;
    private final String tableName;
    private static final Pattern pattern = Pattern.compile("_");

    public LogTable(String tableName) {
        this.tableName = tableName;
        this.tableId = Long.parseLong(pattern.split(tableName, 0)[1]);
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogTable logTable = (LogTable) o;
        return tableId == logTable.tableId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId);
    }

    @Override
    public int compareTo(@NotNull LogTable that) {
        if (tableId == that.tableId) {
            return 0;
        }
        return tableId > that.tableId ? 1 : -1;
    }

}

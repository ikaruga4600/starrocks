// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Config.java
package com.starrocks.journal.jdbc;

import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JdbcJournalCursor implements JournalCursor {

    private static final Logger LOG = LogManager.getLogger(JdbcJournalCursor.class);
    private JdbcEnv environment;
    private long fromKey;
    private long toKey;
    private List<LogTable> logTables;
    private LogTable currentTable;
    private int nextTablePositionIndex;
    private Connection conn;
    private Statement stmt;
    private ResultSet rs;
    private static final String queryTemplate = "select id, op, body from %s where id >= %s order by id asc";

    public static JdbcJournalCursor getJournalCursor(JdbcEnv env, long fromKey, long toKey) {
        if (toKey < fromKey || fromKey < 0) {
            LOG.error("Invalid key range!");
            return null;
        }
        JdbcJournalCursor cursor = null;
        try {
            cursor = new JdbcJournalCursor(env, fromKey, toKey);
        } catch (Exception e) {
            LOG.error("new JdbcJournalCursor error.", e);
        }
        return cursor;
    }

    private JdbcJournalCursor(JdbcEnv env, long fromKey, long toKey) {
        this.environment = env;
        this.toKey = toKey;
        this.fromKey = fromKey;
        this.logTables = env.logTables();
        if (logTables == null) {
            throw new NullPointerException("log tables is null.");
        }

        // find the table which may contain the fromKey
        currentTable = null;
        for (LogTable logTable : logTables) {
            if (fromKey >= logTable.getTableId()) {
                currentTable = logTable;
                nextTablePositionIndex++;
            } else {
                break;
            }
        }
        if (currentTable == null) {
            LOG.error("Can not find the key:{}, fail to get journal cursor. will exit.", fromKey);
            System.exit(-1);
        }
        try {
            conn = environment.createConn();
            stmt = conn.createStatement();
        } catch (SQLException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public JournalEntity next() {
        try {
            if (rs == null) {
                createRS();
            }
            return nextRS();
        } catch (SQLException | IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    private JournalEntity nextRS() throws SQLException, IOException {
        if (rs.next()) {
            return nextJournal();
        }
        environment.silenceClose(rs);
        if (logTables.size() <= nextTablePositionIndex) {
            close();
            return null;
        }
        environment.silenceClose(rs);
        currentTable = logTables.get(nextTablePositionIndex);
        if (currentTable.getTableId() > toKey) {
            close();
            return null;
        }
        nextTablePositionIndex++;
        createRS();
        return nextRS();
    }

    private JournalEntity nextJournal() throws IOException, SQLException {
        JournalEntity entity = new JournalEntity();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(rs.getBytes(3)));
        entity.readFields(in);
        return entity;
    }

    private void createRS() throws SQLException {
        rs = stmt.executeQuery(String.format(queryTemplate, currentTable.getTableName(), fromKey));
    }

    @Override
    public void close() {
        environment.silenceClose(rs);
        environment.silenceClose(stmt);
        environment.silenceClose(conn);
    }

}

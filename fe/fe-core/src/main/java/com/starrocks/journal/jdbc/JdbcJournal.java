// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Config.java
package com.starrocks.journal.jdbc;

import com.starrocks.common.Config;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.metric.MetricRepo;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class JdbcJournal implements Journal {
    public static final Logger LOG = LogManager.getLogger(JdbcJournal.class);
    private static final int OUTPUT_BUFFER_INIT_SIZE = 128;

    private JdbcEnv jdbcEnv;
    private LogTable currentTable;
    private long logId = 0;
    private HikariDataSource ds;

    public JdbcJournal(String nodeName) {
    }

    @Override
    public synchronized void open() {
        if (jdbcEnv != null) {
            return;
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(Config.editlog_jdbc_url);
        config.setUsername(Config.editlog_jdbc_user);
        config.setPassword(Config.editlog_jdbc_pwd);
        config.setDriverClassName("com.mysql.jdbc.Driver");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        try {
            ds = new HikariDataSource(config);
        } catch (Exception e) {
            LOG.error("journal failed", e);
            System.exit(-1);
        }
        this.jdbcEnv = JdbcEnv.create(ds);
        jdbcEnv.init();
        List<LogTable> logTables = jdbcEnv.logTables();
        while (logTables.isEmpty()) {
            // first time startup, no log table be found
            jdbcEnv.createLogTable(1);
            LOG.warn("log tables not found, retry after 1 sec");
            logTables = jdbcEnv.logTables();
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                System.exit(-1);
            }
        }
        logId = getMaxJournalId() + 1;
        jdbcEnv.prepared();
    }

    @Override
    public synchronized void rollJournal() {
        if (count() == 0) {
            return;
        }
        logId = getMaxJournalId() + 1;
        jdbcEnv.createLogTable(logId);
        List<LogTable> logTables = jdbcEnv.logTables();
        currentTable = logTables.get(logTables.size() - 1);
    }

    @Override
    public long getMaxJournalId() {
        if (jdbcEnv == null) {
            return -1;
        }
        List<LogTable> logTables = jdbcEnv.logTables();
        currentTable = logTables.get(logTables.size() - 1);
        return currentTable.getTableId() + count() - 1;
    }

    @Override
    public long getMinJournalId() {
        if (jdbcEnv == null) {
            return -1;
        }
        List<LogTable> logTables = jdbcEnv.logTables();
        if (logTables.isEmpty()) {
            return -1;
        }
        return logTables.get(0).getTableId();
    }

    @Override
    public void close() {
        ds.close();
        ds = null;
    }

    @Override
    public JournalEntity read(long journalId) {
        throw new RuntimeException();
    }

    @Override
    public JournalCursor read(long fromKey, long toKey) {
        return JdbcJournalCursor.getJournalCursor(jdbcEnv, fromKey, toKey);
    }

    @Override
    public synchronized void write(short op, Writable writable) {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(op);
        entity.setData(writable);
        long id = logId++;
        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        try {
            entity.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] data = buffer.getData();
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_EDIT_LOG_SIZE_BYTES.increase((long) data.length);
        }
        LOG.debug("opCode = {}, journal size = {}", op, data.length);
        jdbcEnv.writeEditLog(currentTable.getTableName(), id, op, data);
    }

    @Override
    public void deleteJournals(long deleteJournalToId) {
        jdbcEnv.dropLogTable(deleteJournalToId);
    }

    @Override
    public long getFinalizedJournalId() {
        List<LogTable> tables = jdbcEnv.logTables();
        if (tables.isEmpty()) {
            LOG.error("tables is empty.");
            return 0;
        }
        if (tables.size() < 2) {
            return 0;
        }
        return tables.get(tables.size() - 1).getTableId() - 1;
    }

    @Override
    public List<Long> getDatabaseNames() {
        return jdbcEnv.logTables().stream().map(LogTable::getTableId).collect(Collectors.toList());
    }

    public long count() {
        return jdbcEnv.logCount(currentTable);
    }

    public void frontendReady() {
        jdbcEnv.frontendReady();
    }
}

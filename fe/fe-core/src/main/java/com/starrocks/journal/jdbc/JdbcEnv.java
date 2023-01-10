// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Config.java
package com.starrocks.journal.jdbc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.Config;
import com.starrocks.service.FrontendOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public class JdbcEnv {

    private static final long RETRY_INTERVAL = 300;
    public static final Logger LOG = LogManager.getLogger(JdbcEnv.class);

    private final DataSource ds;

    private static JdbcEnv jdbcEnv;

    private volatile long lastHB;
    private volatile boolean preparing;

    public static JdbcEnv create(DataSource ds) {
        jdbcEnv = new JdbcEnv(ds);
        JdbcHA jdbcHA = new JdbcHA(jdbcEnv);
        Catalog.getCurrentCatalog().setHaProtocol(jdbcHA);
        return jdbcEnv;
    }

    public static JdbcEnv get() {
        return jdbcEnv;
    }

    private JdbcEnv(DataSource ds) {
        this.ds = ds;
    }

    public List<LogTable> logTables() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("show tables like 'log%'");
            List<LogTable> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(new LogTable(rs.getString(1)));
            }
            return tables.stream().sorted().collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("get log table failed", e);
            return null;
        } finally {
            silenceClose(rs);
            silenceClose(stmt);
            silenceClose(conn);
        }
    }

    private static final String createLogTableTemplate = "CREATE TABLE IF NOT EXISTS `log_%s` (\n" +
            "  `id` bigint NOT NULL,\n" +
            "  `op` smallint(6) NOT NULL,\n" +
            "  `body` longblob NOT NULL,\n" +
            "  PRIMARY KEY (`id`)\n" +
            ")";

    public void createLogTable(long tableId) {
        Connection conn = null;
        Statement stmt = null;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                waitMasterHB();
                stmt.execute(String.format(createLogTableTemplate, tableId));
                break;
            } catch (Exception e) {
                LOG.error("", e);
                sleepUntilExit();
            } finally {
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    private static final String createMasterTemplate
            = "insert into master_heartbeat(id, host, hb_ts) values (1, '%s', now(3))";

    public boolean tryMaster() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = ds.getConnection();
            stmt = conn.createStatement();
            long createMasterTs = System.currentTimeMillis();
            stmt.execute(String.format(createMasterTemplate, FrontendOptions.getLocalHostAddress()
                    + ":" + Config.edit_log_port));
        } catch (SQLException e) {
            if (!e.getMessage().startsWith("Duplicate entry")) {
                LOG.warn("try master failed", e);
            }
            return false;
        } finally {
            silenceClose(stmt);
            silenceClose(conn);
        }
        return true;
    }

    public HeartBeat getMasterHeartBeat() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery(
                        "select host, hb_ts, hb_ts < now() - interval 5 second from master_heartbeat");
                if (rs.next()) {
                    HeartBeat hb = new HeartBeat();
                    hb.setNode(rs.getString(1));
                    hb.setTs(rs.getTimestamp(2).getTime());
                    hb.setExpired(rs.getBoolean(3));
                    return hb;
                } else {
                    return null;
                }
            } catch (SQLException e) {
                LOG.warn("", e);
                sleepUntilExit();
            } finally {
                silenceClose(rs);
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    private static final String updateMasterTemplate = "update master_heartbeat " +
            "set host = '%s', hb_ts = now(3) " +
            "where host = '%s' and hb_ts = '%s' ";

    public boolean tryUpdateMaster(HeartBeat lastMasterHB) {
        Connection conn = null;
        Statement stmt = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {
            conn = ds.getConnection();
            stmt = conn.createStatement();
            long updateTs = System.currentTimeMillis();
            int updateCnt = stmt.executeUpdate(String.format(updateMasterTemplate,
                    FrontendOptions.getLocalHostAddress() + ":" + Config.edit_log_port,
                    lastMasterHB.getNode(),
                    sdf.format(lastMasterHB.getTs())));
            return updateCnt != 0;
        } catch (SQLException e) {
            LOG.warn("try update master heartbeat failed", e);
            return false;
        } finally {
            silenceClose(stmt);
            silenceClose(conn);
        }
    }

    private static final String writeEditLogTemplate = "insert into %s values(?, ?, ?)";

    public void writeEditLog(String logTable, long id, short op, byte[] data) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int retryTimes = 0;
        while (true) {
            try {
                conn = ds.getConnection();
                String insertSql = String.format(writeEditLogTemplate, logTable);
                stmt = conn.prepareStatement(insertSql);
                stmt.setLong(1, id);
                stmt.setShort(2, op);
                stmt.setBytes(3, data);
                waitMasterHB();
                stmt.execute();
                break;
            } catch (SQLException e) {
                if (retryTimes++ >= 3) {
                    LOG.error("write edit_log failed. will exit", e);
                    System.exit(-1);
                }
            } finally {
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    public void silenceClose(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignore) {
            }
        }
    }

    private void sleepUntilExit() {
        try {
            TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL);
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            System.exit(-1);
        }
    }

    public void init() {
        preparing = true;
        Connection conn = null;
        Statement stmt = null;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                stmt.execute("CREATE TABLE IF NOT EXISTS `master_heartbeat` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `host` varchar(21) DEFAULT NULL,\n" +
                        "  `hb_ts` timestamp(3) NULL DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ")");
                stmt.execute("CREATE TABLE IF NOT EXISTS `frontend_heartbeat` (\n" +
                        "  `host` varchar(21) NOT NULL,\n" +
                        "  `hb_ts` timestamp(3) NULL DEFAULT NULL,\n" +
                        "  `is_ready` boolean,\n" +
                        "  PRIMARY KEY (`host`)\n" +
                        ")");
                stmt.execute("CREATE TABLE IF NOT EXISTS `epoch` (\n" +
                        "  `epoch` bigint(20) NOT NULL,\n" +
                        "  PRIMARY KEY (`epoch`)\n" +
                        ")");
                break;
            } catch (SQLException e) {
                LOG.info("init jdbc env failed", e);
                sleepUntilExit();
            } finally {
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    public Connection createConn() throws SQLException {
        return ds.getConnection();
    }

    private static final String dropLogTableTemplate = "DROP TABLE IF EXISTS %s";

    public void dropLogTable(long deleteJournalToId) {
        Connection conn = null;
        Statement stmt = null;
        try {
            List<LogTable> logTables = logTables();
            conn = ds.getConnection();
            stmt = conn.createStatement();
            for (LogTable logTable : logTables) {
                if (logTable.getTableId() >= deleteJournalToId) {
                    break;
                }
                stmt.execute(String.format(dropLogTableTemplate, logTable.getTableName()));
            }
        } catch (SQLException e) {
            LOG.warn("drop log table failed", e);
        } finally {
            silenceClose(stmt);
            silenceClose(conn);
        }
    }

    public List<String> frontends() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        int retryTimes = 0;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery(
                        "select host from frontend_heartbeat where hb_ts > now() - interval 5 second and is_ready");
                List<String> frontends = new ArrayList<>();
                while (rs.next()) {
                    frontends.add(rs.getString(1));
                }
                return frontends;
            } catch (SQLException e) {
                if (retryTimes++ >= 3) {
                    LOG.warn("", e);
                    return Lists.newArrayList();
                }
            } finally {
                silenceClose(rs);
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    private static final String addFrontendTemplate = "insert into frontend_heartbeat(host, hb_ts, is_ready) " +
            "values ('%s', now(3), false) ";

    public void addFrontend(String feNode) {
        Connection conn = null;
        Statement stmt = null;
        int retryTimes = 0;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                stmt.execute(String.format(addFrontendTemplate, feNode));
                break;
            } catch (SQLException e) {
                if (retryTimes++ >= 3) {
                    LOG.warn("", e);
                    throw new RuntimeException("add frontend failed " + e.getMessage(), e);
                }
            } finally {
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    private static final String frontendReadyTemplate = "update frontend_heartbeat set is_ready = true " +
            "where host = '%s' ";

    public void frontendReady() {
        Connection conn = null;
        Statement stmt = null;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                stmt.execute(String.format(frontendReadyTemplate,
                        FrontendOptions.getLocalHostAddress() + ":" + Config.edit_log_port));
                break;
            } catch (SQLException e) {
                LOG.warn("", e);
                sleepUntilExit();
            } finally {
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    private static final String dropFrontendTemplate = "delete frontend_heartbeat where host = '%s' ";

    public void dropFrontend(String feNode) {
        Connection conn = null;
        Statement stmt = null;
        int retryTimes = 0;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                stmt.execute(String.format(dropFrontendTemplate, feNode));
                break;
            } catch (SQLException e) {
                if (retryTimes++ >= 3) {
                    LOG.warn("", e);
                    throw new RuntimeException("drop frontend failed " + e.getMessage(), e);
                }
            } finally {
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    private static final String reportNonMasterHeartBeatTemplate = "update frontend_heartbeat " +
            "set hb_ts = now(3) " +
            "where is_ready and host = ? ";

    public void reportNonMasterHeartBeat() {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ds.getConnection();
            stmt = conn.prepareStatement(reportNonMasterHeartBeatTemplate);
            stmt.setString(1, FrontendOptions.getLocalHostAddress() + ":" + Config.edit_log_port);
            stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.info("frontend hb report failed", e);
        } finally {
            silenceClose(stmt);
            silenceClose(conn);
        }
    }

    private static final String reportMasterHeartBeatTemplate = "update master_heartbeat " +
            "set hb_ts = now(3) " +
            "where host = ? ";

    public boolean reportMasterHeartBeat() {
        Connection conn = null;
        PreparedStatement stmt = null;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.prepareStatement(reportMasterHeartBeatTemplate);
                stmt.setString(1, FrontendOptions.getLocalHostAddress() + ":" + Config.edit_log_port);
                return stmt.executeUpdate() != 0;
            } catch (SQLException e) {
                LOG.info("master hb report failed", e);
                sleepUntilExit();
            } finally {
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    private static final String incrEpochTemplate = "update epoch set epoch = epoch + 1 where epoch = ?";

    public long epochGetAndIncrement() {
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int retryTimes = 0;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery("select epoch from epoch");
                if (rs.next()) {
                    long curr = rs.getLong(1);
                    ps = conn.prepareStatement(incrEpochTemplate);
                    ps.setLong(1, curr);
                    boolean updateSuccess = ps.executeUpdate() != 0;
                    return updateSuccess ? curr + 1 : -1;
                } else {
                    stmt.executeUpdate("insert into epoch values (1)");
                    return 1;
                }
            } catch (SQLException e) {
                if (retryTimes++ >= 3) {
                    LOG.info("get epoch failed", e);
                    return -1;
                }
            } finally {
                silenceClose(rs);
                silenceClose(ps);
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    public long logCount(LogTable currentTable) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        while (true) {
            try {
                conn = ds.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery("select count(1) from " + currentTable.getTableName());
                rs.next();
                return rs.getLong(1);
            } catch (SQLException e) {
                LOG.info("get journal table cnt failed", e);
                sleepUntilExit();
            } finally {
                silenceClose(rs);
                silenceClose(stmt);
                silenceClose(conn);
            }
        }
    }

    public void updateLastMasterHB() {
        lastHB = System.currentTimeMillis();
    }

    public void waitMasterHB() {
        while (!preparing && System.currentTimeMillis() - lastHB >= Config.jdbc_edit_log_master_report_delay_tolerate) {
            LOG.warn("editlog write block at master hb {}", lastHB);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void prepared() {
        preparing = false;
    }
}

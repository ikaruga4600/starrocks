// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Config.java
package com.starrocks.journal.jdbc;

import com.starrocks.catalog.Catalog;
import com.starrocks.ha.HAProtocol;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class JdbcHA implements HAProtocol {

    private final JdbcEnv jdbcEnv;

    public JdbcHA(JdbcEnv jdbcEnv) {
        this.jdbcEnv = jdbcEnv;
    }

    @Override
    public long getEpochNumber() {
        return 0;
    }

    @Override
    public boolean fencing() {
        long epoch = jdbcEnv.epochGetAndIncrement();
        if (epoch < 0) {
            return false;
        }
        Catalog.getCurrentCatalog().setEpoch(epoch);
        return true;
    }

    @Override
    public List<InetSocketAddress> getObserverNodes() {
        return new ArrayList<>();
    }

    private static final Pattern colonSplitPattern = Pattern.compile(":");

    @Override
    public List<InetSocketAddress> getElectableNodes(boolean leaderIncluded) {
        List<String> frontends = jdbcEnv.frontends();
        Set<String> hosts = new HashSet<>();
        List<InetSocketAddress> result = new ArrayList<>();
        for (String host : frontends) {
            if (!hosts.add(host)) {
                continue;
            }
            String[] ipPort = colonSplitPattern.split(host);
            result.add(new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1])));
        }
        if (leaderIncluded) {
            InetSocketAddress master = getLeader();
            if (!hosts.contains(master.getHostName() + ":" + master.getPort())) {
                result.add(master);
            }
        }
        return result;
    }

    @Override
    public InetSocketAddress getLeader() {
        HeartBeat heartBeat = jdbcEnv.getMasterHeartBeat();
        if (heartBeat.isExpired()) {
            return null;
        }
        String host = heartBeat.getNode();
        String[] ipPort = colonSplitPattern.split(host);
        return new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
    }

    @Override
    public List<InetSocketAddress> getNoneLeaderNodes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void transferToMaster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void transferToNonMaster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLeader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeElectableNode(String nodeName) {
        return true;
    }

    public void dropNode(String nodeName) {
        jdbcEnv.dropFrontend(formatNodeName(nodeName));
    }

    public void addNode(String nodeName) {
        jdbcEnv.addFrontend(formatNodeName(nodeName));
    }

    private static final Pattern underlineSplitPattern = Pattern.compile("_");

    private String formatNodeName(String nodeName) {
        String[] nodeNameSplit = underlineSplitPattern.split(nodeName);
        return nodeNameSplit[0] + ":" + nodeNameSplit[1];
    }

}

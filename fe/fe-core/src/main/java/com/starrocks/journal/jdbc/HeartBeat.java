// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Config.java
package com.starrocks.journal.jdbc;

public class HeartBeat {
    private String node;
    private long ts;
    private boolean expired;

    public String getNode() {
        return node;
    }

    public HeartBeat setNode(String node) {
        this.node = node;
        return this;
    }

    public long getTs() {
        return ts;
    }

    public HeartBeat setTs(long ts) {
        this.ts = ts;
        return this;
    }

    public boolean isExpired() {
        return expired;
    }

    public HeartBeat setExpired(boolean expired) {
        this.expired = expired;
        return this;
    }
}

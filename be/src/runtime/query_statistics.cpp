// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/query_statistics.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/query_statistics.h"

#include "util/scoped_cleanup.h"

namespace starrocks {

void QueryStatistics::merge(QueryStatisticsRecvr* recvr) {
    recvr->merge(this);
}

void QueryStatisticsRecvr::insert(const PQueryStatistics& statistics, int sender_id) {
    std::lock_guard<SpinLock> l(_lock);
    QueryStatistics* query_statistics = nullptr;
    auto iter = _query_statistics.find(sender_id);
    if (iter == _query_statistics.end()) {
        query_statistics = new QueryStatistics;
        auto cleanup = MakeScopedCleanup([&]() { delete query_statistics; });
        _query_statistics[sender_id] = query_statistics;
        cleanup.cancel();
    } else {
        query_statistics = iter->second;
    }
    query_statistics->merge_pb(statistics);
}

QueryStatisticsRecvr::~QueryStatisticsRecvr() {
    std::lock_guard<SpinLock> l(_lock);
    for (auto& pair : _query_statistics) {
        delete pair.second;
    }
    _query_statistics.clear();
}

} // namespace starrocks

// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/result_buffer_mgr.h

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

#ifndef STARROCKS_BE_RUNTIME_RESULT_BUFFER_MGR_H
#define STARROCKS_BE_RUNTIME_RESULT_BUFFER_MGR_H

#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "util/uid_util.h"

namespace starrocks {

class TFetchDataResult;
class BufferControlBlock;
class GetResultBatchCtx;
class PUniqueId;

// manage all result buffer control block in one backend
class ResultBufferMgr {
public:
    ResultBufferMgr();
    ~ResultBufferMgr();
    // init Result Buffer Mgr, start cancel thread
    Status init();
    // create one result sender for this query_id
    // the returned sender do not need release
    // sender is not used when call cancel or unregister
    Status create_sender(const TUniqueId& query_id, int buffer_size, std::shared_ptr<BufferControlBlock>* sender);
    // fetch data, used by RPC
    Status fetch_data(const TUniqueId& fragment_id, std::unique_ptr<TFetchDataResult>* result);

    void fetch_data(const PUniqueId& finst_id, GetResultBatchCtx* ctx);

    // cancel
    Status cancel(const TUniqueId& fragment_id);

    // cancel one query at a future time.
    Status cancel_at_time(time_t cancel_time, const TUniqueId& query_id);

private:
    typedef boost::unordered_map<TUniqueId, std::shared_ptr<BufferControlBlock>> BufferMap;
    typedef std::map<time_t, std::vector<TUniqueId>> TimeoutMap;

    std::shared_ptr<BufferControlBlock> find_control_block(const TUniqueId& query_id);

    // used to erase the buffer that fe not clears
    // when fe crush, this thread clear the buffer avoid memory leak in this backend
    void cancel_thread();

    bool _is_stop{false};
    // lock for buffer map
    std::mutex _lock;
    // buffer block map
    BufferMap _buffer_map;

    // lock for timeout map
    std::mutex _timeout_lock;

    // map (cancel_time : query to be cancelled),
    // cancel time maybe equal, so use one list
    TimeoutMap _timeout_map;

    std::unique_ptr<boost::thread> _cancel_thread;
};

// TUniqueId hash function used for boost::unordered_map
std::size_t hash_value(const TUniqueId& fragment_id);
} // namespace starrocks

#endif

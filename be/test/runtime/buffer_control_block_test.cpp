// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/buffer_control_block_test.cpp

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

#include "runtime/buffer_control_block.h"

#include <gtest/gtest.h>
#include <pthread.h>

#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class BufferControlBlockTest : public testing::Test {
public:
    BufferControlBlockTest() {}
    virtual ~BufferControlBlockTest() {}

protected:
    virtual void SetUp() {}

private:
};

TEST_F(BufferControlBlockTest, init_normal) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());
}

TEST_F(BufferControlBlockTest, add_one_get_one) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
    add_result->result_batch.rows.push_back("hello test");
    ASSERT_TRUE(control_block.add_batch(std::move(add_result)).ok());

    auto get_result = std::make_unique<TFetchDataResult>();
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_FALSE(get_result->eos);
    ASSERT_EQ(1U, get_result->result_batch.rows.size());
    ASSERT_STREQ("hello test", get_result->result_batch.rows[0].c_str());
}

TEST_F(BufferControlBlockTest, get_one_after_close) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    control_block.close(Status::OK());
    auto get_result = std::make_unique<TFetchDataResult>();
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_TRUE(get_result->eos);
}

TEST_F(BufferControlBlockTest, get_add_after_cancel) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    ASSERT_TRUE(control_block.cancel().ok());
    std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
    add_result->result_batch.rows.push_back("hello test");
    ASSERT_FALSE(control_block.add_batch(std::move(add_result)).ok());

    auto get_result = std::make_unique<TFetchDataResult>();
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());
}

void* cancel_thread(void* param) {
    BufferControlBlock* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    control_block->cancel();
    return NULL;
}

TEST_F(BufferControlBlockTest, add_then_cancel) {
    // only can add one batch
    BufferControlBlock control_block(TUniqueId(), 1);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, NULL, cancel_thread, &control_block);

    {
        std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
        add_result->result_batch.rows.push_back("hello test1");
        add_result->result_batch.rows.push_back("hello test2");
        ASSERT_TRUE(control_block.add_batch(std::move(add_result)).ok());
    }
    {
        std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
        add_result->result_batch.rows.push_back("hello test1");
        add_result->result_batch.rows.push_back("hello test2");
        ASSERT_FALSE(control_block.add_batch(std::move(add_result)).ok());
    }

    auto get_result = std::make_unique<TFetchDataResult>();
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());

    pthread_join(id, NULL);
}

TEST_F(BufferControlBlockTest, get_then_cancel) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, NULL, cancel_thread, &control_block);

    // get block until cancel
    auto get_result = std::make_unique<TFetchDataResult>();
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());

    pthread_join(id, NULL);
}

void* add_thread(void* param) {
    BufferControlBlock* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    {
        std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
        add_result->result_batch.rows.push_back("hello test1");
        add_result->result_batch.rows.push_back("hello test2");
        control_block->add_batch(std::move(add_result));
    }
    return NULL;
}

TEST_F(BufferControlBlockTest, get_then_add) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, NULL, add_thread, &control_block);

    // get block until a batch add
    auto get_result = std::make_unique<TFetchDataResult>();
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_FALSE(get_result->eos);
    ASSERT_EQ(2U, get_result->result_batch.rows.size());
    ASSERT_STREQ("hello test1", get_result->result_batch.rows[0].c_str());
    ASSERT_STREQ("hello test2", get_result->result_batch.rows[1].c_str());

    pthread_join(id, NULL);
}

void* close_thread(void* param) {
    BufferControlBlock* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    control_block->close(Status::OK());
    return NULL;
}

TEST_F(BufferControlBlockTest, get_then_close) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, NULL, close_thread, &control_block);

    // get block until a batch add
    auto get_result = std::make_unique<TFetchDataResult>();
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_TRUE(get_result->eos);
    ASSERT_EQ(0U, get_result->result_batch.rows.size());

    pthread_join(id, NULL);
}

} // namespace starrocks

/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */

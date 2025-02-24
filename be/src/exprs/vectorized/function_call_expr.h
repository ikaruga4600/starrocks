// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/builtin_functions.h"

namespace starrocks {
namespace vectorized {

class VectorizedFunctionCallExpr final : public Expr {
public:
    explicit VectorizedFunctionCallExpr(const TExprNode& node);

    ~VectorizedFunctionCallExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedFunctionCallExpr(*this)); }

protected:
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* context) override;

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    bool is_constant() const override;

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override;

private:
    const FunctionDescriptor* _fn_desc;

    bool _is_returning_random_value;
};

} // namespace vectorized
} // namespace starrocks

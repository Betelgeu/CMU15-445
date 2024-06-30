//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "aggregation_executor.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */

/**
 * A simplified hash table that has all the necessary functionality for window functions
 */
class SimpleWindowHashTable {
 public:
  explicit SimpleWindowHashTable(WindowFunctionType win_type) : win_type_(win_type) {}

  /**
  @return The initial window aggregate value for this window executor
  */
  auto GenerateInitialWindowValue() -> Value {
    switch (win_type_) {
      case WindowFunctionType::CountStarAggregate:
        return ValueFactory::GetIntegerValue(0);
      case WindowFunctionType::Rank:
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
    }
  }
  /**
   * Combines the input into the aggregation result.
   * @param[out] result The output rows of aggregate value corresponding to one key
   * @param input The input value
   */
  void CombineWindowValues(Value *result, const Value &input) {
    auto &value = *result;
    auto &input_value = input;

    // 不插入空值, 空值除了初始化，不能做任何改变操作
    if (input_value.IsNull()) {
      return;
    }

    switch (win_type_) {
      case WindowFunctionType::CountStarAggregate:
        value = value.Add(input_value);
        break;
      case WindowFunctionType::CountAggregate:
        if (value.IsNull()) {
          value = ValueFactory::GetIntegerValue(1);
        } else {
          value = value.Add(ValueFactory::GetIntegerValue(1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (value.IsNull()) {
          value = input_value;
        } else {
          value = value.Add(input_value);
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (value.IsNull()) {
          value = input_value;
        } else {
          value = value.CompareLessThan(input_value) == CmpBool::CmpTrue ? value : input_value;
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (value.IsNull()) {
          value = input_value;
        } else {
          value = value.CompareGreaterThan(input_value) == CmpBool::CmpTrue ? value : input_value;
        }
        break;
      // rank和count(*)一样, input固定是1
      case WindowFunctionType::Rank:
        if (value.IsNull()) {
          value = input_value;
        } else {
          value = value.Add(input_value);
        }
        break;
    }
  }
  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const Value &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialWindowValue()});
    }
    CombineWindowValues(&ht_[agg_key], agg_val);
  }

  auto OutputWinValue(const AggregateKey &agg_key) -> Value { return ht_[agg_key]; }

  auto CompareOrderBy(const AggregateKey &agg_key, const AggregateKey &now_order_by_value) -> bool {
    if (ht_last_order_.count(agg_key) == 0) {
      return false;
    }
    auto last_order = ht_last_order_[agg_key];
    for (uint32_t i = 0; i < last_order.group_bys_.size(); i++) {
      if (last_order.group_bys_[i].CompareEquals(now_order_by_value.group_bys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }

  std::unordered_map<AggregateKey, AggregateKey> ht_last_order_;
  std::unordered_map<AggregateKey, int> ht_last_order_count_;

 private:
  WindowFunctionType win_type_;
  std::unordered_map<AggregateKey, Value> ht_;
};

class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple, std::vector<AbstractExpressionRef> &partition_by) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_by.size());
    for (const auto &expr : partition_by) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple, AbstractExpressionRef &function) -> Value {
    return function->Evaluate(tuple, child_executor_->GetOutputSchema());
  }

  auto MakeAggregateValueFromOrderBy(const Tuple *tuple) -> AggregateKey {
    auto order_by = plan_->window_functions_.begin()->second.order_by_;
    std::vector<AbstractExpressionRef> order_by_exprs;
    order_by_exprs.reserve(order_by.size());
    for (const auto &pair : order_by) {
      order_by_exprs.push_back(pair.second);
    }
    return MakeAggregateKey(tuple, order_by_exprs);
  }

  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::unordered_map<uint32_t, SimpleWindowHashTable> win_ht_;

  std::vector<Tuple> output_tuples_;
  size_t output_idx_;
};

}  // namespace bustub

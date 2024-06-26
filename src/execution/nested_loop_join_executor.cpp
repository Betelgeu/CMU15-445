//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

auto GenerateOutputTuple(std::vector<Value> &left_values, std::vector<Value> &right_values, const Schema *schema) -> Tuple {
  std::vector<Value> values;
  values.reserve(left_values.size() + right_values.size());
  values.insert(values.end(), left_values.begin(), left_values.end());
  values.insert(values.end(), right_values.begin(), right_values.end());
  Tuple output_tuple(values, schema);
  return output_tuple;
}

void NestedLoopJoinExecutor::Init() {
  // 可能需要多次init, 但是只需要聚合计算一次
  output_idx_ = 0;
  if(!tuples_.empty()) {
    return;
  }

  // 在init中就计算得到所有输出tuple并保存
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;

  auto &left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto &right_schema = plan_->GetRightPlan()->OutputSchema();

  left_executor_->Init();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    // values = left_values + right_values
    std::vector<Value> left_values;
    left_values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount());
    for(uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
      left_values.push_back(left_tuple.GetValue(&left_schema, i));
    }

    right_executor_->Init();
    bool found = false;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto join_value = plan_->predicate_->EvaluateJoin(&left_tuple, left_schema,
                                                              &right_tuple, right_schema);
      // 找到符合条件的inner tuple
      if (!join_value.IsNull() && join_value.GetAs<bool>()) {
        found = true;
        std::vector<Value> right_values;
        right_values.reserve(plan_->GetRightPlan()->OutputSchema().GetColumnCount());
        for(uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          right_values.push_back(right_tuple.GetValue(&right_schema, i));
        }
        Tuple output_tuple = GenerateOutputTuple(left_values, right_values, &plan_->OutputSchema());
        tuples_.push_back(output_tuple);
      }
    }

    // 没有符合条件的inner tuple
    if (!found) {
      std::vector<Value> right_values;
      right_values.reserve(plan_->GetRightPlan()->OutputSchema().GetColumnCount());
      if(plan_->GetJoinType() == JoinType::LEFT) {
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          right_values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
        }

      }
      else if(plan_->GetJoinType() == JoinType::INNER) {
        continue;
      }
      Tuple output_tuple = GenerateOutputTuple(left_values, right_values, &plan_->OutputSchema());
      tuples_.push_back(output_tuple);
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_idx_ < tuples_.size()) {
    *tuple = tuples_[output_idx_];
    output_idx_++;
    return true;
  }
  return false;
}

}  // namespace bustub

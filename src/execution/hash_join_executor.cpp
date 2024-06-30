//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <vector>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

auto MakeAggregateKey(const std::vector<AbstractExpressionRef> &exprs, const Tuple *tuple, const Schema *schema)
    -> AggregateKey {
  std::vector<Value> vals;
  vals.reserve(exprs.size());
  for (const auto &expr : exprs) {
    vals.emplace_back(expr->Evaluate(tuple, *schema));
  }
  return {vals};
}

void HashJoinExecutor::Init() {
  output_idx_ = 0;
  if (!output_tuples_.empty()) {
    return;
  }

  // make inner hash table
  Tuple right_tuple;
  RID right_rid;
  right_child_->Init();
  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto key = MakeAggregateKey(plan_->RightJoinKeyExpressions(), &right_tuple, &right_child_->GetOutputSchema());
    ht_.insert({key, right_tuple});
  }
  // check outer tuple
  Tuple left_tuple;
  RID left_rid;
  left_child_->Init();
  while (left_child_->Next(&left_tuple, &left_rid)) {
    auto key = MakeAggregateKey(plan_->LeftJoinKeyExpressions(), &left_tuple, &left_child_->GetOutputSchema());
    auto range = ht_.equal_range(key);
    std::vector<Tuple> tuples;
    for (auto it = range.first; it != range.second; ++it) {
      auto right_tuple = it->second;

      std::vector<Value> values;
      auto left_size = left_child_->GetOutputSchema().GetColumnCount();
      auto right_size = right_child_->GetOutputSchema().GetColumnCount();
      values.reserve(left_size + right_size);
      for (size_t i = 0; i < left_size; i++) {
        values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_size; i++) {
        values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }
      tuples.emplace_back(values, &GetOutputSchema());
    }

    if (tuples.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      auto left_size = left_child_->GetOutputSchema().GetColumnCount();
      auto right_size = right_child_->GetOutputSchema().GetColumnCount();
      values.reserve(left_size + right_size);
      for (size_t i = 0; i < left_size; i++) {
        values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_size; i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      tuples.emplace_back(values, &GetOutputSchema());
    }

    output_tuples_.insert(output_tuples_.end(), tuples.begin(), tuples.end());
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_idx_ < output_tuples_.size()) {
    *tuple = output_tuples_[output_idx_];
    output_idx_++;
    return true;
  }
  return false;
}

}  // namespace bustub

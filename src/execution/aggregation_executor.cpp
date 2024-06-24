//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  // int idx = 0;
  while(child_executor_->Next(&tuple, &rid)){
    // Get the key and value
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    // Insert the key and value into the hash table
    aht_.InsertCombine(key, value);
  }

  // 处理空表
  if(aht_.Begin() == aht_.End()){
    // 插入一个初始化<key, value>
    // key无所谓
    // value必须是GenerateInitialAggregateValue生成的初始值
    auto key = MakeAggregateKey(&tuple);
    auto value = aht_.GenerateInitialAggregateValue();
    aht_.InsertCombine(key, value);
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    auto key = aht_iterator_.Key();
    auto value = aht_iterator_.Val();
    // SELECT colA, MIN(colB) FROM __mock_table_1 GROUP BY colA;

    // Generate the output tuple
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount()); // 预留空间
    // values = key + value
    values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
    values.insert(values.end(), value.aggregates_.begin(), value.aggregates_.end());
    *tuple = Tuple(values, &GetOutputSchema());

    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub

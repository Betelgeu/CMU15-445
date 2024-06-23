//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  called_ = false;
}
// delete + insert
// 删除只需要修改tuple_meta的is_deleted

auto CompareKey(Tuple &key1, Tuple &key2, Schema &key_schema) -> bool {
  for(uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
    auto value1 = key1.GetValue(&key_schema, i);
    auto value2 = key2.GetValue(&key_schema, i);
    if(value1.CompareNotEquals(value2) == CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple temp_tuple;
  RID temp_rid;
  int size = 0;
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    // update to table heap
    auto &table = table_info_->table_;
    // 1. delete
    table->UpdateTupleMeta({0, true}, temp_rid);
    // 2. update && insert
    std::vector<Value> values;
    for(uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      auto new_value = plan_->target_expressions_[i]->Evaluate(&temp_tuple, table_info_->schema_);
      values.emplace_back(new_value);
    }
    auto new_tuple = Tuple(values, &table_info_->schema_);
    auto rid_opt = table->InsertTuple({0, false}, new_tuple);
    if(!rid_opt.has_value()) {
      throw Exception("Insert failed");
    }
    size++;
    auto new_rid = rid_opt.value();

    // update index
    // 只更新改变列上的index
    auto table_name = table_info_->name_;
    auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for(auto index_info : index_info_vec) {
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto old_key = temp_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      // key没有改变，则不需要更新
      if(CompareKey(new_key, old_key, index_info->key_schema_)) {
        continue;
      }
      index_info->index_->DeleteEntry(old_key, temp_rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    }

  }
  // output size tuple
  std::vector<Value> values;
  values.emplace_back(INTEGER, size);
  *tuple = Tuple(values, &GetOutputSchema());
  // output once
  if(!called_) {
    called_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub

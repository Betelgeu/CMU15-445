//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  called_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto &table = table_info->table_;
  int size = 0;
  Tuple temp_tuple;
  RID temp_rid;
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    table->UpdateTupleMeta({0, true}, temp_rid);
    size++;

    // delete from index
    auto table_name = table_info->name_;
    auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for (auto index_info : index_info_vec) {
      auto key =
          temp_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, temp_rid, exec_ctx_->GetTransaction());
    }
  }

  // output size tuple
  std::vector<Value> values;
  values.emplace_back(INTEGER, size);
  *tuple = Tuple(values, &GetOutputSchema());
  // output once
  if (!called_) {
    called_ = true;
    return true;
  }
  return false;
}

}  // namespace bustub

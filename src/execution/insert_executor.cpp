//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

// 注意child的init()
void InsertExecutor::Init() {
  insert_table_id_ = plan_->GetTableOid();
  child_executor_->Init();
  called_ = false;
}

// p3暂时不考虑transaction, 在MVCC中使用
// 这里暂时不使用rid
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int size = 0;
  Tuple insert_tuple;
  RID insert_rid;
  auto &insert_table = exec_ctx_->GetCatalog()->GetTable(insert_table_id_)->table_;
  while(child_executor_->Next(&insert_tuple, &insert_rid)) {
    // 这里由child给的rid可能是无效的
    // 例如value(0, 1, 2)创建的tuple只存在于临时表中，不是真正的insert_rid
    // insert_rid在InsertTuple后生成

    // insert to table heap
    TupleMeta meta = {0, false};
    auto rid_opt = insert_table->InsertTuple(meta, insert_tuple);
    if(rid_opt.has_value()) {
      insert_rid = rid_opt.value();
      size++;
    } else {
      throw Exception("Insert failed");
    }

    // update index
    auto insert_table_info = exec_ctx_->GetCatalog()->GetTable(insert_table_id_);
    auto insert_table_name = insert_table_info->name_;
    auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(insert_table_name);
    for(auto index_info : index_info_vec) {
      auto key = insert_tuple.KeyFromTuple(insert_table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, insert_rid, exec_ctx_->GetTransaction());
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

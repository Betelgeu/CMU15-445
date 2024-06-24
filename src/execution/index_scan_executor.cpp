//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // 从index中拿到所有的rid

  // 1. get index
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  // auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  auto &index = index_info->index_;
  // 2. get key
  auto key_schema = index_info->key_schema_;
  auto key_value = plan_->pred_key_->Evaluate(nullptr, key_schema);
  auto key_tuple = Tuple({key_value}, &key_schema);
  // get all rid
  index->ScanKey(key_tuple, &results_, nullptr);
  idx_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto &table = table_info->table_;
  while (idx_ < static_cast<int>(results_.size())) {
    auto temp_rid = results_[idx_++];
    auto [temp_meta, temp_tuple] = table->GetTuple(temp_rid);
    auto value = plan_->filter_predicate_->Evaluate(&temp_tuple, table_info->schema_);
    // get value
    auto status = value.GetAs<bool>();
    // do not emit tuples that are deleted
    status &= !temp_meta.is_deleted_;
    if (status) {
      *rid = temp_rid;
      *tuple = temp_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub

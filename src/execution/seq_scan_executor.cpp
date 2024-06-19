//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto table_id = plan_->GetTableOid();
  auto table_heap = exec_ctx_->GetCatalog()->GetTable(table_id)->table_.get();
  table_iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(table_iter_->IsEnd()) {
    return false;
  }
  // 到第一个有效tuple或者end
  while(!table_iter_->IsEnd() && table_iter_->GetTuple().first.is_deleted_) {
    ++(*table_iter_);
  }

  if(!table_iter_->IsEnd()) {
    *tuple = table_iter_->GetTuple().second;
    *rid = table_iter_->GetRID();
    ++(*table_iter_);
    return true;
  }
  return false;
}

}  // namespace bustub

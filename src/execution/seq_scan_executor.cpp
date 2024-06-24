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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_id = plan_->GetTableOid();
  auto &table_heap = exec_ctx_->GetCatalog()->GetTable(table_id)->table_;
  table_iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 到第一个有效tuple或者end
  while (true) {
    bool status = !table_iter_->IsEnd();
    if (!status) {
      return false;
    }

    auto [temp_meta, temp_tuple] = table_iter_->GetTuple();
    auto temp_rid = table_iter_->GetRID();
    status &= !temp_meta.is_deleted_;
    // 可能做了谓词下推
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&temp_tuple, plan_->OutputSchema());
      status &= value.GetAs<bool>();
    }

    ++(*table_iter_);
    if (status) {
      *tuple = temp_tuple;
      *rid = temp_rid;
      return true;
    }
  }
}

}  // namespace bustub

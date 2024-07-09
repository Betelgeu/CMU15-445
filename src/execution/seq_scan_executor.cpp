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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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

    // MVCC
    auto undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(temp_rid).value();
    auto read_ts = exec_ctx_->GetTransaction()->GetReadTs();
    auto ts = temp_meta.ts_;
    std::vector<UndoLog> undo_logs;
    while(read_ts < ts && undo_link.IsValid()) {
      if(ts == exec_ctx_->GetTransaction()->GetTransactionId()) {
        break;
      }
      auto undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(undo_link);
      undo_logs.push_back(undo_log);
      undo_link = undo_log.prev_version_;
      ts = undo_log.ts_;
    }
    auto tuple_opt = ReconstructTuple(&plan_->OutputSchema(), temp_tuple, temp_meta, undo_logs);
    if(read_ts < ts || !tuple_opt.has_value()) {
      ++(*table_iter_);
      continue;
    }
    temp_tuple = tuple_opt.value();

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

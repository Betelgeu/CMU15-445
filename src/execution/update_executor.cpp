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
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
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
  for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
    auto value1 = key1.GetValue(&key_schema, i);
    auto value2 = key2.GetValue(&key_schema, i);
    if (value1.CompareNotEquals(value2) == CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {

  Tuple base_tuple;
  RID base_rid;
  int size = 0;
  auto &table = table_info_->table_;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto read_ts = txn->GetReadTs();
  auto schema = table_info_->schema_;

  while (child_executor_->Next(&base_tuple, &base_rid)) {
    std::vector<Value> values;
    for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      auto new_value = plan_->target_expressions_[i]->Evaluate(&base_tuple, table_info_->schema_);
      values.emplace_back(new_value);
    }
    auto new_tuple = Tuple(values, &table_info_->schema_);
    const auto & [old_meta, old_tuple] = table->GetTuple(base_rid);
    auto ts = old_meta.ts_;
    auto modified_field = GetModifiedField(new_tuple, old_tuple, &schema);
    //check MVCC write conflict
    if (ts == txn->GetTransactionId()) {
      table->UpdateTupleInPlace({txn->GetTransactionId(), false}, new_tuple, base_rid);
      // 如果存在undo log, 需要将其覆盖更新
      if(txn_manager->GetUndoLink(base_rid).has_value()) {
        auto undo_link = txn_manager->GetUndoLink(base_rid).value();
        auto old_undo_log = txn_manager->GetUndoLog(undo_link);
        auto delta_tuple = DeltaTuple(base_tuple, &schema, modified_field);
        UndoLog new_undo_log = CoverUndoLog(delta_tuple, &schema, modified_field, old_undo_log);
        txn->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
      }
    }
    else if (read_ts < ts) {
      txn->SetTainted();
      throw ExecutionException("update: MVCC write-write conflict");
    }
    else {
      // 创建undo log, 更新版本链
      auto base_meta = table->GetTupleMeta(base_rid);
      UndoLink old_undo_link;
      if(txn_manager->GetUndoLink(base_rid).has_value()) {
        old_undo_link = txn_manager->GetUndoLink(base_rid).value();
      }
      UndoLink new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};
      auto delta_tuple = DeltaTuple(base_tuple, &schema, modified_field);
      UndoLog new_undo_log = {base_meta.is_deleted_, modified_field, delta_tuple, base_meta.ts_, old_undo_link};
      txn->AppendUndoLog(new_undo_log);
      txn_manager->UpdateUndoLink(base_rid, new_undo_link);

      //update in place
      table->UpdateTupleInPlace({txn->GetTransactionId(), false}, new_tuple, base_rid);
    }

    txn->AppendWriteSet(plan_->table_oid_, base_rid);
    size++;
    // update index
    // 只更新改变列上的index
    auto table_name = table_info_->name_;
    auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
    for (auto index_info : index_info_vec) {
      auto new_key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto old_key =
          base_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      // key没有改变，则不需要更新
      if (CompareKey(new_key, old_key, index_info->key_schema_)) {
        continue;
      }
      index_info->index_->DeleteEntry(old_key, base_rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key, base_rid, exec_ctx_->GetTransaction());
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

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
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  called_ = false;
}

auto NullTuple(Schema *schema) -> Tuple {
  std::vector<Value> values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    auto type = schema->GetColumn(i).GetType();
    values.emplace_back(ValueFactory::GetNullValueByType(type));
  }
  return {values, schema};
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto &table = table_info->table_;
  int size = 0;
  Tuple base_tuple;
  RID base_rid;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto read_ts = txn->GetReadTs();
  timestamp_t ts;
  while (child_executor_->Next(&base_tuple, &base_rid)) {
    //check MVCC write conflict
    auto base_meta = table->GetTupleMeta(base_rid);
    ts = table->GetTupleMeta(base_rid).ts_;
    if (ts == txn->GetTransactionId()) {
      table->UpdateTupleMeta({txn->GetTransactionId(), true}, base_rid);
    }
    else if (read_ts < ts) {
      txn->SetTainted();
      throw ExecutionException("delete: MVCC write-write conflict");
    }
    else {
      // 创建undo log, 更新版本链
      // 对delete而言, 所有字段都被修改，所以modification_field全为true
      auto schema = table_info->schema_;
      auto all_modified_field = std::vector<bool>(schema.GetColumnCount(), true);
      UndoLink old_undo_link;
      if(txn_manager->GetUndoLink(base_rid).has_value()) {
        old_undo_link = txn_manager->GetUndoLink(base_rid).value();
      }
      UndoLink new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};
      auto delta_tuple = DeltaTuple(base_tuple, &schema, all_modified_field);
      UndoLog new_undo_log = {base_meta.is_deleted_, all_modified_field, delta_tuple, base_meta.ts_, old_undo_link};
      txn->AppendUndoLog(new_undo_log);
      txn_manager->UpdateUndoLink(base_rid, new_undo_link);

      // 更新base tuple meta
      table->UpdateTupleMeta({txn->GetTransactionId(), true}, base_rid);

      // delete from index
      auto table_name = table_info->name_;
      auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
      for (auto index_info : index_info_vec) {
        auto key =
            base_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(key, base_rid, exec_ctx_->GetTransaction());
      }
    }
    size++;
    // 更新write set
    txn->AppendWriteSet(plan_->table_oid_, base_rid);
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

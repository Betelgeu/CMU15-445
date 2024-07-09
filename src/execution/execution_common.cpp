#include "execution/execution_common.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  uint32_t col_len = schema->GetColumnCount();
  if(!base_meta.is_deleted_){
    for(uint32_t i = 0; i < col_len; i++) {
      values.push_back(base_tuple.GetValue(schema, i));
    }
  }

  for(const auto& undo_log : undo_logs){
    BUSTUB_ASSERT(undo_log.modified_fields_.size() == col_len, "modified_fields should have the same column size");
    if(undo_log.is_deleted_) {
      values.clear();
      continue;
    }
    // 1. build partial tuple schema
    std::vector<uint32_t> attrs;
    for(uint32_t i = 0; i < col_len; i++) {
      if(undo_log.modified_fields_[i]) {
        attrs.push_back(i);
      }
    }
    Schema partial_schema = Schema::CopySchema(schema, attrs);
    // 2. reconstruct values
    std::vector<Value> partial_values;
    partial_values.reserve(attrs.size());
    for(uint32_t i = 0; i < attrs.size(); i++) {
      partial_values.push_back(undo_log.tuple_.GetValue(&partial_schema, i));
    }
    // 3. update values
    if(values.empty()) {
      for(const auto & col : schema->GetColumns()) {
        values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
    }
    for(uint32_t i = 0; i < attrs.size(); i++) {
      values[attrs[i]] = partial_values[i];
    }
  }

  if(values.size() == col_len) {
    return Tuple(values, schema);
  }
  return std::nullopt;
}

void PrintUndoLog(const UndoLog &undo_log, const Schema *schema) {
  if(undo_log.is_deleted_) {
    fmt::print("<del>");
  } else {
    uint32_t col_len = schema->GetColumnCount();
    std::vector<uint32_t> attrs;
    for(uint32_t i = 0; i < col_len; i++) {
      if(undo_log.modified_fields_[i]) {
        attrs.push_back(i);
      }
    }
    Schema partial_schema = Schema::CopySchema(schema, attrs);
    fmt::print("(");
    for(uint32_t i = 0, j = 0; i < col_len; i++) {
      if(undo_log.modified_fields_[i]) {
        auto value = undo_log.tuple_.GetValue(&partial_schema, j++);
        if(value.IsNull()) {
          fmt::print("<NULL>");
        }
        else {
          fmt::print("{}", value.ToString());
        }
      }
      else {
        fmt::print("_");
      }

      if(i != col_len - 1) {
        fmt::print(", ");
      }
    }
    fmt::print(")");
  }
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  auto table_iter = std::make_unique<TableIterator>(table_heap->MakeIterator());
  auto schema = table_info->schema_;
  while(!table_iter->IsEnd()) {
    // print base tuple
    auto [base_meta, base_tuple] = table_iter->GetTuple();
    auto rid = table_iter->GetRID();
    fmt::print(stderr, "RID={}/{} ", rid.GetPageId(), rid.GetSlotNum());
    if(base_meta.ts_ < TXN_START_ID) {
      fmt::print(stderr, "ts={} ", base_meta.ts_);
    }
    else {
      fmt::print(stderr, "ts=txn{} ", base_meta.ts_ - TXN_START_ID);
    }

    if(base_meta.is_deleted_) {
      fmt::print(stderr, "<del marker> ");
    }
    fmt::print(stderr, "tuple={}\n", base_tuple.ToString(&schema));
    // print whole version chain
    auto undo_link = txn_mgr->GetUndoLink(rid).value();
    while(undo_link.IsValid()) {
      auto undo_log = txn_mgr->GetUndoLog(undo_link);
      fmt::print(stderr, "  txn{}@{} ", undo_link.prev_txn_ - TXN_START_ID, undo_link.prev_log_idx_);
      PrintUndoLog(undo_log, &schema);
      fmt::print(stderr, " ts={}\n", undo_log.ts_);

      undo_link = undo_log.prev_version_;
    }
    ++(*table_iter);
  }
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub

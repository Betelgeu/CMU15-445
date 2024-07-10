//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <cstdint>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // 需要用到原子操作
  txn_ref->read_ts_.store(last_commit_ts_.load());
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  // update the tuple meta in write set
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for(auto &[table_id, rids] : txn->GetWriteSets()) {
    auto &table_heap = catalog_->GetTable(table_id)->table_;
    for(auto &rid : rids) {
      const auto &[base_meta, base_tuple] = table_heap->GetTuple(rid);
      table_heap->UpdateTupleInPlace({commit_ts, base_meta.is_deleted_}, base_tuple, rid);
    }
  }

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  last_commit_ts_.fetch_add(1);
  txn->commit_ts_.store(last_commit_ts_.load());

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto water_mark = GetWatermark();
  std::vector<int64_t> txn_ids_to_remove;
  for(auto [txn_id, txn_ref] : txn_map_) {
    int undo_log_num = txn_ref->GetUndoLogNum();
    int invisible_undo_log_num = 0;
    for(auto [table_id, rids] : txn_ref->GetWriteSets()) {
      for(auto rid : rids) {
        bool found_last = false;
        auto & table_heap = catalog_->GetTable(table_id)->table_;
        auto base_meta = table_heap->GetTupleMeta(rid);
        // 沿着版本链找下去，第二个小于等于water_mark的版本就是invisible的，这其中还要是这个txn的undo log才行
        if(base_meta.ts_ <= water_mark) {
          found_last = true;
        }
        auto undo_link_opt = GetUndoLink(rid);
        if(undo_link_opt.has_value()) {
          auto undo_link = undo_link_opt.value();
          while(undo_link.IsValid()) {
            auto undo_log_opt = GetUndoLogOptional(undo_link);
            if(!undo_log_opt.has_value()) {
              break;
            }
            auto undo_log = undo_log_opt.value();
            auto ts = undo_log.ts_;
            auto undo_log_txn_id = undo_link.prev_txn_;

            if(ts <= water_mark) {
              if(!found_last) {
                found_last = true;
              }
              else if(txn_id == undo_log_txn_id) {
                invisible_undo_log_num++;
                break;
              }
            }

            undo_link = undo_log.prev_version_;
          }
        }

      }
    }
    if(invisible_undo_log_num == undo_log_num &&
      (txn_ref->GetTransactionState() == TransactionState::COMMITTED || txn_ref->GetTransactionState() == TransactionState::ABORTED)
    ) {
      txn_ids_to_remove.push_back(txn_id);
    }
  }

  for(auto txn_id : txn_ids_to_remove) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <fmt/format.h>
#include <mutex>
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t fid, size_t k) : k_(k), fid_(fid), prev_(nullptr), next_(nullptr) {}
LRUKNode::LRUKNode(frame_id_t fid, size_t k, LRUKNode *prev, LRUKNode *next)
    : k_(k), fid_(fid), prev_(prev), next_(next) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  dummy_head_ = new LRUKNode(-1, k);
  dummy_tail_ = new LRUKNode(-1, k);
  dummy_head_->next_ = dummy_tail_;
  dummy_tail_->prev_ = dummy_head_;
}
LRUKReplacer::~LRUKReplacer() {
  delete dummy_head_;
  delete dummy_tail_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> latch(latch_);
  // store the output frame_id(evicted frame_id) in the pointer
  if (curr_size_ <= 0) {
    return false;
  }
  // 直接从链表尾部开始，找到第一个可以被替换的frame
  LRUKNode *cur = dummy_tail_->prev_;
  while (cur != dummy_head_) {
    if (cur->is_evictable_) {
      *frame_id = cur->fid_;
      cur->prev_->next_ = cur->next_;
      cur->next_->prev_ = cur->prev_;
      node_store_.erase(*frame_id);
      curr_size_--;
      return true;
    }
    cur = cur->prev_;
  }
  throw Exception(ExceptionType::EXECUTION, "Can't find a Frame to evict");
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id >= static_cast<int>(replacer_size_)) {
    throw Exception(ExceptionType::EXECUTION, "Frame id out of range");
  }

  std::unique_lock<std::mutex> latch(latch_);
  // update the access history of the frame
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.insert({frame_id, LRUKNode(frame_id, k_)});
  } else {
    LRUKNode &node = node_store_[frame_id];
    node.prev_->next_ = node.next_;
    node.next_->prev_ = node.prev_;
  }

  node_store_[frame_id].history_.push_back(current_timestamp_);
  if (node_store_[frame_id].history_.size() > k_) {
    node_store_[frame_id].history_.pop_front();
  }
  current_timestamp_++;

  //插入链表中相应位置
  LRUKNode &node = node_store_[frame_id];
  LRUKNode *prev;
  LRUKNode *cur;
  // 不足k个的记录
  // 若不存在其他也是不足k个的，则插入到成为第一个不足k个的
  // 否则，在不足k个的node中，按照lru的方法插入
  if (node.history_.size() < k_) {
    prev = dummy_head_;
    cur = dummy_head_->next_;
    while (cur != dummy_tail_) {
      if (cur->history_.size() < k_ && cur->history_.front() < node.history_.front()) {
        break;
      }
      prev = cur;
      cur = cur->next_;
    }
  }
  // 满足k个记录则插入到链表中间相应位置
  else {
    prev = dummy_head_;
    cur = dummy_head_->next_;
    while (cur != dummy_tail_ && cur->history_.size() == k_) {
      if ((current_timestamp_ - cur->history_.front()) >= (current_timestamp_ - node.history_.front())) {
        break;
      }
      prev = cur;
      cur = cur->next_;
    }
  }
  // 将node插入到perv之后
  prev->next_ = &node;
  node.prev_ = prev;
  node.next_ = cur;
  cur->prev_ = &node;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> latch(latch_);
  if (frame_id >= static_cast<int>(replacer_size_)) {
    throw Exception(ExceptionType::EXECUTION, "Frame id out of range");
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
    // throw Exception(ExceptionType::EXECUTION, fmt::format("Frame {} not found", frame_id));
  }

  LRUKNode &node = node_store_[frame_id];
  if (set_evictable) {
    if (!node.is_evictable_) {
      curr_size_++;
    }
    node.is_evictable_ = true;
  } else {
    if (node.is_evictable_) {
      curr_size_--;
    }
    node.is_evictable_ = false;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> latch(latch_);
  // remove the specific frame_id from the replacer
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (!node_store_[frame_id].is_evictable_) {
    throw Exception(ExceptionType::EXECUTION, fmt::format("Frame {} is non-evictable", frame_id));
  }

  // remove the frame_id from the replacer
  LRUKNode &node = node_store_[frame_id];
  node.prev_->next_ = node.next_;
  node.next_->prev_ = node.prev_;
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> latch(latch_);
  return curr_size_;
}

}  // namespace bustub

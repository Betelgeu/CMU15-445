//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  this->max_depth_ = max_depth;
  this->global_depth_ = 0;
  uint32_t num_directory_pages = 0x1 << max_depth_;
  for (uint32_t i = 0; i < num_directory_pages; i++) {
    this->bucket_page_ids_[i] = INVALID_PAGE_ID;
    this->local_depths_[i] = 0;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // 取hash的低global_depth_位, 作为index
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // 前local_depth - 1位相同，第local_depth位不同
  // 这里用异或实现
  return bucket_idx ^ (0x1 << (local_depths_[bucket_idx] - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (0x1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  uint32_t size = Size();
  uint32_t incr_idx = 0x1 << global_depth_;
  for (uint32_t i = 0; i < size; i++, incr_idx++) {
    uint32_t bucket_idx = i;
    SetBucketPageId(incr_idx, bucket_page_ids_[bucket_idx]);
    SetLocalDepth(incr_idx, GetLocalDepth(bucket_idx));
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 0x1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  auto local_depth = local_depths_[bucket_idx];
  return (0x1 << local_depth) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub

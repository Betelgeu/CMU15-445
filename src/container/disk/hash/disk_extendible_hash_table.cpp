//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // only init the header page
  index_name_ = name;
  BasicPageGuard header_guard = bpm->NewPageGuarded(&this->header_page_id_);
  auto head_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  head_page->Init(header_max_depth);
  // std::cout << "name: " << name << std::endl;
  // std::cout << "header_max_depth: " << header_max_depth << std::endl;
  // std::cout << "directory_max_depth: " << directory_max_depth << std::endl;
  // std::cout << "bucket_max_size: " << bucket_max_size << std::endl;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = Hash(key);
  // 1. load header page
  ReadPageGuard header_read_guard = this->bpm_->FetchPageRead(this->header_page_id_);
  auto head_page = header_read_guard.As<ExtendibleHTableHeaderPage>();
  auto directory_page_idx = head_page->HashToDirectoryIndex(hash);
  auto directory_page_id = head_page->GetDirectoryPageId(directory_page_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }
  header_read_guard.Drop();
  // 2. load correspond directory page
  ReadPageGuard directory_read_guard = this->bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_read_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_page_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_page_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_read_guard.Drop();

  // 3. load correspond bucket page
  ReadPageGuard bucket_read_guard = this->bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_read_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (!bucket_page->Lookup(key, value, this->cmp_)) {
    return false;
  }
  result->push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // PrintHT();
  // std::cout << "insert: " << key << " " << value << std::endl;
  auto hash = Hash(key);
  // 1. load header page & directory page & bucket page
  WritePageGuard header_write_guard = this->bpm_->FetchPageWrite(this->header_page_id_);
  auto header_page = header_write_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_page_idx = header_page->HashToDirectoryIndex(hash);
  if (header_page->GetDirectoryPageId(directory_page_idx) == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return InsertToNewDirectory(header_page, directory_page_idx, hash, key, value);
  }
  auto directory_page_id = header_page->GetDirectoryPageId(directory_page_idx);
  header_write_guard.Drop();
  WritePageGuard directory_write_guard = this->bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_page_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_page_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_page_idx, key, value);
  }
  WritePageGuard bucket_write_guard = this->bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  while (!bucket_page->Insert(key, value, this->cmp_)) {
    // 2. lookup & if the bucket is not full, insert directly
    // 这里只支持unique key-value pairs
    // 遇到重复key不用管value是不是也一样，直接返回false
    V temp;
    if (bucket_page->Lookup(key, temp, this->cmp_)) {
      return false;
    }

    // 3. if the bucket is full, split the bucket->rehash->reinsert
    // std::cout << "split" << std::endl;
    directory_page->IncrLocalDepth(bucket_page_idx);
    auto split_image_idx = directory_page->GetSplitImageIndex(bucket_page_idx);
    if (directory_page->GetLocalDepth(bucket_page_idx) > directory_page->GetGlobalDepth()) {
      // std::cout << "double directory" << std::endl;
      directory_page->IncrGlobalDepth();
      if (directory_page->GetGlobalDepth() > directory_page->GetMaxDepth()) {
        return false;
      }
    }
    page_id_t split_image_page_id;
    WritePageGuard split_image_guard = this->bpm_->NewPageGuarded(&split_image_page_id).UpgradeWrite();
    directory_page->SetBucketPageId(split_image_idx, split_image_page_id);
    directory_page->SetLocalDepth(split_image_idx, directory_page->GetLocalDepth(bucket_page_idx));
    auto split_image_page = split_image_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    split_image_page->Init(this->bucket_max_size_);
    // rearrange the directory
    auto local_depth = directory_page->GetLocalDepth(bucket_page_idx);
    auto diff = 0x1 << local_depth;
    for (uint32_t i = bucket_page_idx; i < directory_page->Size(); i += diff) {
      directory_page->SetBucketPageId(i, bucket_page_id);
      directory_page->SetLocalDepth(i, local_depth);
    }
    for (uint32_t i = split_image_idx; i < directory_page->Size(); i += diff) {
      directory_page->SetBucketPageId(i, split_image_page_id);
      directory_page->SetLocalDepth(i, local_depth);
    }
    for (int32_t i = bucket_page_idx; i >= 0; i -= diff) {
      directory_page->SetBucketPageId(i, bucket_page_id);
      directory_page->SetLocalDepth(i, local_depth);
    }
    for (int32_t i = split_image_idx; i >= 0; i -= diff) {
      directory_page->SetBucketPageId(i, split_image_page_id);
      directory_page->SetLocalDepth(i, local_depth);
    }
    // rehash
    int size = bucket_page->Size();
    int local_depth_mask = directory_page->GetLocalDepthMask(bucket_page_idx);
    for (int i = 0; i < size; i++) {
      auto key = bucket_page->KeyAt(0);
      auto value = bucket_page->ValueAt(0);
      bucket_page->RemoveAt(0);
      auto hash = Hash(key);
      auto new_bucket_idx = hash & local_depth_mask;
      if (new_bucket_idx == split_image_idx) {
        split_image_page->Insert(key, value, this->cmp_);
      } else {
        bucket_page->Insert(key, value, this->cmp_);
      }
    }
    // reinsert
    auto new_bucket_idx = hash & local_depth_mask;
    if (new_bucket_idx == split_image_idx) {
      bucket_page = std::move(split_image_page);
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // 1. create a new directory page
  page_id_t new_directory_page_id;
  this->bpm_->NewPageGuarded(&new_directory_page_id);
  WritePageGuard directory_write_guard = this->bpm_->FetchPageWrite(new_directory_page_id);
  auto directory_page = directory_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(this->directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_directory_page_id);
  // 2. insert key-value pair to a new bucket page
  auto new_bucket_idx = directory_page->HashToBucketIndex(hash);
  return InsertToNewBucket(directory_page, new_bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // 1. create&load a new bucket page
  page_id_t new_bucket_page_id;
  this->bpm_->NewPageGuarded(&new_bucket_page_id);
  WritePageGuard bucket_write_guard = this->bpm_->FetchPageWrite(new_bucket_page_id);
  auto bucket_page = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(this->bucket_max_size_);
  // 2. insert key-value pair to the new bucket page
  bucket_page->Insert(key, value, this->cmp_);
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // PrintHT();
  // std::cout << "remove: " << key << std::endl;
  // 1. load->search & remove
  auto hash = Hash(key);
  ReadPageGuard header_read_guard = this->bpm_->FetchPageRead(this->header_page_id_);
  auto head_page = header_read_guard.As<ExtendibleHTableHeaderPage>();
  auto directory_page_idx = head_page->HashToDirectoryIndex(hash);
  auto directory_page_id = head_page->GetDirectoryPageId(directory_page_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }
  header_read_guard.Drop();
  WritePageGuard directory_write_guard = this->bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_page_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_page_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_write_guard = this->bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto ans = static_cast<bool>(bucket_page->Remove(key, this->cmp_));
  // 2. try to merge or shrink
  while (ans && bucket_page->Size() == 0) {
    auto local_depth = directory_page->GetLocalDepth(bucket_page_idx);
    // 所有bucket_idx都指向同一个bucket, 没必要merge
    if (local_depth == 0) {
      break;
    }
    auto split_image_idx = directory_page->GetSplitImageIndex(bucket_page_idx);
    auto split_image_page_id = directory_page->GetBucketPageId(split_image_idx);
    if (split_image_idx >= directory_page->Size() || split_image_page_id == bucket_page_id ||
        split_image_idx == bucket_page_idx) {
      break;
    }
    if (local_depth == directory_page->GetLocalDepth(split_image_idx)) {
      // std::cout << "merge" << std::endl;
      auto diff = 1 << local_depth;
      for (uint32_t i = bucket_page_idx; i < directory_page->Size(); i += diff) {
        directory_page->SetBucketPageId(i, split_image_page_id);
        directory_page->DecrLocalDepth(i);
      }
      for (int32_t i = bucket_page_idx - diff; i >= 0; i -= diff) {
        directory_page->SetBucketPageId(i, split_image_page_id);
        directory_page->DecrLocalDepth(i);
      }
      for (uint32_t i = split_image_idx; i < directory_page->Size(); i += diff) {
        directory_page->DecrLocalDepth(i);
      }
      for (int32_t i = split_image_idx - diff; i >= 0; i -= diff) {
        directory_page->DecrLocalDepth(i);
      }
      this->bpm_->DeletePage(bucket_page_id);
    }

    // recursively merge
    bucket_write_guard.Drop();
    WritePageGuard split_image_guard = this->bpm_->FetchPageWrite(split_image_page_id);
    auto split_image_page = split_image_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (split_image_page->Size() == 0) {
      bucket_page = std::move(split_image_page);
      bucket_page_id = split_image_page_id;
      bucket_page_idx = split_image_idx;
      continue;
    }
    split_image_guard.Drop();
    // try to shrink
    if (directory_page->CanShrink()) {
      // 检查shrink范围内有没有空bucket, 有的话先merge这些页面
      bool merge = false;
      for (uint64_t idx = directory_page->Size() / 2; idx < directory_page->Size(); idx++) {
        auto check_page_id = directory_page->GetBucketPageId(idx);
        if (check_page_id == INVALID_PAGE_ID) {
          continue;
        }
        WritePageGuard check_write_guard = this->bpm_->FetchPageWrite(check_page_id);
        auto check_page = check_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        if (check_page->Size() == 0) {
          bucket_page = std::move(check_page);
          bucket_page_id = check_page_id;
          bucket_page_idx = idx;
          merge = true;
          break;
        }
      }
      if (merge) {
        continue;
      }
      while (directory_page->CanShrink()) {
        // shrink
        // std::cout << "shrink" << std::endl;
        directory_page->DecrGlobalDepth();
      }
    }
    break;
  }
  return ans;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

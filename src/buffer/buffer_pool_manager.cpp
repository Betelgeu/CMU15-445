//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <memory>
#include <mutex>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));  // frame id
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// 准备向磁盘中写入写入新的page, 返回新的page的指针
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // try to prepare a frame for the new page
  std::unique_lock<std::mutex> lock(latch_);
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }
  *page_id = AllocatePage();
  // select frame to load page
  // use free page
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  // evit the page
  else {
    if (!replacer_->Evict(&frame_id)) {
      throw Exception(ExceptionType::EXECUTION, "Can't find a Frame to evict");
    }
    Page &evict_page = pages_[frame_id];
    if (pages_[frame_id].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({/*is_write=*/true, evict_page.GetData(), evict_page.GetPageId(), std::move(promise)});
      if (!future.get()) {
        throw Exception(ExceptionType::EXECUTION, "Write evict page failed");
      }
    }
    page_table_.erase(evict_page.GetPageId());
    pages_[frame_id].ResetMemory();  // we don't read the page from disk, so we need to reset the memory
  }
  // reset metadata of the new page
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  page_table_[*page_id] = frame_id;
  // pin the page
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  return &pages_[frame_id];
}

// 准备从磁盘中读取page_id对应的page
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // page_id needs to be fetched from the disk but all frames are currently in use and not evictable
  // similar to NewPage, but we need to load the page from disk
  std::unique_lock<std::mutex> lock(latch_);
  // page is not in buffer pool, need to load from disk firstly
  if (page_table_.find(page_id) == page_table_.end()) {
    if (replacer_->Size() == 0 && free_list_.empty()) {
      return nullptr;
    }
    // select frame to load page
    // use free page
    frame_id_t frame_id;
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    }
    // evit the page
    else {
      if (!replacer_->Evict(&frame_id)) {
        throw Exception(ExceptionType::EXECUTION, "Can't find a Frame to evict");
      }
      Page &evict_page = pages_[frame_id];
      if (pages_[frame_id].IsDirty()) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        disk_scheduler_->Schedule(
            {/*is_write=*/true, evict_page.GetData(), evict_page.GetPageId(), std::move(promise)});
        if (!future.get()) {
          throw Exception(ExceptionType::EXECUTION, "Write evict page failed");
        }
      }
      page_table_.erase(evict_page.GetPageId());
    }
    // reset metadata of the new page
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    // load page from disk
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(
        {/*is_write=*/false, /*page data pointer*/ pages_[frame_id].GetData(), page_id, std::move(promise)});
    if (!future.get()) {
      throw Exception(ExceptionType::EXECUTION, "Read page failed");
    }
    page_table_[page_id] = frame_id;
  }

  frame_id_t frame_id = page_table_[page_id];
  // pin the page
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  // If page_id is not in the buffer pool or its pin count is already 0, return false.
  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].pin_count_ <= 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ |= is_dirty;  // other thread maybe wrote the page earlier
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

// 将page_id对应的page写入磁盘
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, pages_[frame_id].GetData(), page_id, std::move(promise)});
  if (!future.get()) {
    throw Exception(ExceptionType::EXECUTION, "Write page failed");
  }
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &pair : page_table_) {
    auto page_id = pair.first;
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  // reset memory and metadata of the page
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub

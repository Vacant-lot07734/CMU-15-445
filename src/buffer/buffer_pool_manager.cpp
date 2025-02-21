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
#include <mutex>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // 重在从buffer pool分配新页面
  std::scoped_lock<std::mutex> latch(latch_);
  frame_id_t replace_frame_id;
  if (!free_list_.empty()) {
    replace_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&replace_frame_id)) {
    page_id = nullptr;
    return nullptr;
  } else {
    auto &cur = pages_[replace_frame_id];
    if (cur.IsDirty()) {
      WriteTodisk(cur.page_id_);
    }
    page_table_.erase(cur.page_id_);
  }
  auto new_page_id = AllocatePage();  //虚拟页号
  *page_id = new_page_id;
  PageReset(replace_frame_id);

  pages_[replace_frame_id].page_id_ = new_page_id;
  pages_[replace_frame_id].pin_count_++;
  page_table_[new_page_id] = replace_frame_id;
  replacer_->SetEvictable(replace_frame_id, false);
  replacer_->RecordAccess(replace_frame_id);
  return &pages_[replace_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // 从buffer pool获取特定页面，不在缓冲池的话，就从磁盘取出来
  std::scoped_lock<std::mutex> latch(latch_);
  // First search for page_id in the buffer pool
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    pages_[iter->second].pin_count_++;
    replacer_->SetEvictable(iter->second, false);
    replacer_->RecordAccess(iter->second, access_type);
    return &pages_[iter->second];
  }
  // pick a replacement frame from either the free list or the replacer
  frame_id_t replace_frame_id;
  if (!free_list_.empty()) {
    replace_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&replace_frame_id)) {
    return nullptr;
  } else {
    auto &cur = pages_[replace_frame_id];
    if (cur.IsDirty()) {
      WriteTodisk(cur.page_id_);
    }
    page_table_.erase(cur.page_id_);
  }
  PageReset(replace_frame_id);
  // read the page from disk by scheduling a read DiskRequest with
  //  disk_scheduler_->Schedule(), and replace the old page in the frame.
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest r{false, pages_[replace_frame_id].GetData(), page_id, std::move(promise)};
  disk_scheduler_->Schedule(std::move(r));
  page_table_[page_id] = replace_frame_id;
  replacer_->SetEvictable(replace_frame_id, false);
  replacer_->RecordAccess(replace_frame_id, access_type);
  pages_[replace_frame_id].pin_count_++;
  pages_[replace_frame_id].page_id_ = page_id;
  future.get();
  return &pages_[replace_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> latch(latch_);
  // If page_id is not in the buffer pool or its pin count is already 0, return false.
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    if (pages_[iter->second].pin_count_ != 0) {
      if (--pages_[iter->second].pin_count_ == 0) {
        replacer_->SetEvictable(iter->second, true);
      }
      if (is_dirty && !pages_[iter->second].is_dirty_) {
        pages_[iter->second].is_dirty_ = true;
      }
      return true;
    }
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> latch(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    WriteTodisk(iter->first);
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> latch(latch_);
  for (auto &it : page_table_) {
    WriteTodisk(it.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> latch(latch_);
  // If page_id is not in the buffer pool, do nothing and return true. If the
  // page is pinned and cannot be deleted, return false immediately.
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    if (pages_[iter->second].pin_count_ != 0) {
      return false;
    }
    // After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
    // back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
    // imitate freeing the page on the disk.

    // if (pages_[iter->second].is_dirty_) {
    //   WriteTodisk(iter->first);
    // }
    // replacer_->Remove(iter->second);
    free_list_.push_back(iter->second);
    PageReset(iter->second);
    pages_[iter->second].page_id_ = INVALID_PAGE_ID;
    //最后再erase，否则上面使用iter，会造成指针悬空
    page_table_.erase(page_id);
    DeallocatePage(page_id);
  }
  return true;
}

void BufferPoolManager::WriteTodisk(page_id_t page_id) {
  auto &frame_id = page_table_[page_id];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest r{true, pages_[frame_id].GetData(), page_id, std::move(promise)};
  disk_scheduler_->Schedule(std::move(r));
  future.get();  //阻塞，直至后台进程完成request
  pages_[frame_id].is_dirty_ = false;
}
void BufferPoolManager::PageReset(frame_id_t frame_id) {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // pin_count++ 升级时才会上锁
  return {this, this->FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // pin_count++ 并上锁
  auto new_page = this->FetchPage(page_id);
  new_page->RLatch();
  return {this, new_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto new_page = this->FetchPage(page_id);
  new_page->WLatch();
  return {this, new_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // NewPage 在buffer pool 分配一个新页面
  return {this, this->NewPage(page_id)};
}

}  // namespace bustub

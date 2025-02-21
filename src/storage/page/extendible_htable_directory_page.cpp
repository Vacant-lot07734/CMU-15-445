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

#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  this->max_depth_ = std::min(static_cast<uint32_t>(HTABLE_DIRECTORY_MAX_DEPTH), max_depth);
  this->global_depth_ = 0;
  for (uint32_t i = 0; i < MaxSize(); i++) {
    this->bucket_page_ids_[i] = INVALID_PAGE_ID;
    this->local_depths_[i] = 0;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  if (global_depth_ == 0) {
    return 0;
  }
  //此处哈希取低位
  // return (hash & (1 << this->global_depth_)-1);
  //先交gd，再交ld，因为ld可能小于gd
  uint32_t local_hash = (hash & (1 << this->global_depth_) - 1);
  // local_hash = local_hash & (this->GetLdMask(local_hash));
  return local_hash;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= MaxSize()) {
    throw Exception("ExtendibleHTableDirectoryPage::GetBucketPageId---bucket_idx is greater than max_depth_");
  }
  return this->bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= MaxSize()) {
    throw Exception("ExtendibleHTableDirectoryPage::SetBucketPageId---bucket_idx is greater than max_depth_");
  }
  this->bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // 分裂后的两个，从一个可以获取另一个
  uint32_t tmp = bucket_idx + (1 << (this->global_depth_ - 1));
  auto mask = this->GetGlobalDepthMask();
  return tmp & mask;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return this->global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  //已经增长到最大
  if (this->global_depth_ == this->max_depth_) {
    return;
  }
  uint32_t t = 1 << this->global_depth_;
  memcpy(this->bucket_page_ids_ + t, this->bucket_page_ids_, t * 4);
  memcpy(this->local_depths_ + t, this->local_depths_, t);
  this->global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (this->global_depth_ == 0) {
    return;
  }
  // 抹除
  // const uint32_t new_size = 1 << (global_depth_ - 1);
  // for (uint32_t i = new_size; i < Size(); ++i) {
  //   bucket_page_ids_[i] = INVALID_PAGE_ID;
  //   local_depths_[i] = 0;
  // }
  this->global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (this->global_depth_ == 0) {
    return false;
  }
  for (uint32_t i = 0; i < Size(); ++i) {
    // only when gd > ld , CanShrink
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << this->global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= MaxSize()) {
    throw Exception("ExtendibleHTableDirectoryPage::GetLocalDepth---bucket_idx is greater than max_depth_");
  }
  return this->local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= MaxSize()) {
    throw Exception("ExtendibleHTableDirectoryPage::SetLocalDepth---bucket_idx is greater than global_depth");
  }
  this->local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= MaxSize()) {
    throw("ExtendibleHTableDirectoryPage::IncrLocalDepth---bucket_idx is greater than global_depth");
  }
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (bucket_idx >= MaxSize()) {
    throw Exception("ExtendibleHTableDirectoryPage::DecrLocalDepth---bucket_idx is greater than global_depth");
  }
  if (local_depths_[bucket_idx] > 0) {
    local_depths_[bucket_idx]--;
  }
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << (this->max_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << this->local_depths_[bucket_idx]) - 1;
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << this->global_depth_) - 1; }

}  // namespace bustub

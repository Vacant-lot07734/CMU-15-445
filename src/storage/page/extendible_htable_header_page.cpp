//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  this->max_depth_ = std::min((uint32_t)HTABLE_HEADER_MAX_DEPTH, max_depth);
  for(u_int32_t i = 0; i < MaxSize(); i++){
    this->directory_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  //uint32_t 应该是跨平台固定32位
  //此处哈希取高位
  return hash >> (32 - this->max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  if(directory_idx >= MaxSize()){
    throw Exception("ExtendibleHTableHeaderPage::GetDirectoryPageId---directory_idx is greater than max_depth");
  }
  return this->directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  if(directory_idx >= MaxSize()){
    throw Exception("ExtendibleHTableHeaderPage::SetDirectoryPageId---directory_idx is greater than max_depth");
  }
  this->directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  return 1 << (this->max_depth_);
}

}  // namespace bustub

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
#include <cstddef>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t id) : k_(k), fid_(id) {}

auto LRUKNode::Getframe() { return this->fid_; }

auto LRUKNode::GetnumOfHist() { return this->history_.size(); }

auto LRUKNode::GetfirstOfHist() { return this->history_.front(); }

auto LRUKNode::IsEvictable() -> bool { return this->is_evictable_; }

void LRUKNode::Record(size_t timestamp) {
  this->history_.push_back(timestamp);
  if (this->history_.size() > this->k_) {
    this->history_.pop_front();
  }
}

void LRUKNode::SetEvictable(bool flag) { this->is_evictable_ = flag; }
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k), replacer_size_(num_frames) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool flag(false);
  bool have_inf(false);
  bool have_k(false);
  std::pair<frame_id_t, size_t> inf_candidate;
  std::pair<frame_id_t, size_t> k_candidate;
  for (auto &v : node_store_) {
    if (!v.second.IsEvictable()) {
      continue;
    }
    if (v.second.GetnumOfHist() < k_) {
      if (!have_inf || (v.second.GetfirstOfHist() < inf_candidate.second)) {
        inf_candidate.first = v.first;
        inf_candidate.second = v.second.GetfirstOfHist();
        have_inf = true;
      }
    } else if (!have_inf) {
      if (!have_k || (v.second.GetfirstOfHist() < k_candidate.second)) {
        k_candidate.first = v.first;
        k_candidate.second = v.second.GetfirstOfHist();
        have_k = true;
      }
    }
  }
  if (have_inf) {
    flag = true;
    *frame_id = inf_candidate.first;
    node_store_.erase(*frame_id);
    curr_size_--;
  } else if (have_k) {
    flag = true;
    *frame_id = k_candidate.first;
    node_store_.erase(*frame_id);
    curr_size_--;
  }
  return flag;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (auto id = static_cast<size_t>(frame_id); id > replacer_size_) {
    throw ExecutionException("不合法id,invalid frame_id");
  }
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    LRUKNode node(k_, frame_id);
    node.Record(current_timestamp_);
    node_store_.emplace(frame_id, node);
  } else {
    iter->second.Record(current_timestamp_);
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (auto id = static_cast<size_t>(frame_id); id > replacer_size_) {
    throw ExecutionException("不合法id,invalid frame_id");
  }
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  if (iter->second.IsEvictable() && !set_evictable) {
    iter->second.SetEvictable(false);
    curr_size_--;
  } else if (!iter->second.IsEvictable() && set_evictable) {
    iter->second.SetEvictable(true);
    curr_size_++;
  }
}
//强制移除evictavle frame，因为缓冲池移除了
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (auto id = static_cast<size_t>(frame_id); id > replacer_size_) {
    return;
  }
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  if (!iter->second.IsEvictable()) {
    throw ExecutionException("non-ecictavle frame cann't be removed");
  }
  node_store_.erase(iter->first);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub

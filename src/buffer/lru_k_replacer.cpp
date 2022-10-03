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
#include <iostream>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  history_queue_ = std::unordered_map<frame_id_t, std::list<size_t>>();
  cache_queue_ = std::unordered_map<frame_id_t, std::list<size_t>>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // initialize frame_id with an invalid value
  *frame_id = INVALID_FRAME_ID;
  current_timestamp_++;
  if (history_queue_.empty() && cache_queue_.empty()) {
    return false;
  }
  size_t ts = current_timestamp_;
  if (!history_queue_.empty()) {
    for (auto kv_pair : history_queue_) {
      if (evictable_.find(kv_pair.first) != evictable_.end() && kv_pair.second.front() < ts) {
        *frame_id = kv_pair.first;
        ts = kv_pair.second.front();
      }
    }
  }
  if (history_queue_.find(*frame_id) != history_queue_.end()) {
    history_queue_.erase(*frame_id);
    evictable_.erase(*frame_id);
    return true;
  }
  for (auto kv_pair : cache_queue_) {
    if (evictable_.find(kv_pair.first) != evictable_.end() && kv_pair.second.front() < ts) {
      *frame_id = kv_pair.first;
      ts = kv_pair.second.front();
    }
  }
  if (*frame_id == INVALID_FRAME_ID) {
    return false;
  }
  cache_queue_.erase(*frame_id);
  evictable_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id >= static_cast<int>(replacer_size_)) {
    return;
  }

  if (cache_queue_.find(frame_id) != cache_queue_.end()) {
    cache_queue_[frame_id].pop_front();
    cache_queue_[frame_id].push_back(current_timestamp_++);
    return;
  }

  if (history_queue_.find(frame_id) == history_queue_.end()) {
    history_queue_[frame_id] = std::list<size_t>();
  }

  // insert into history queue
  history_queue_[frame_id].push_back(current_timestamp_++);

  // move it to cache queue if accessed more than k times
  if (history_queue_[frame_id].size() >= k_) {
    cache_queue_[frame_id] = history_queue_[frame_id];
    history_queue_.erase(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  current_timestamp_++;
  if (set_evictable && history_queue_.find(frame_id) == history_queue_.end() &&
      cache_queue_.find(frame_id) == cache_queue_.end()) {
    return;
  }
  if (set_evictable) {
    evictable_[frame_id] = true;
  } else {
    evictable_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  current_timestamp_++;

  /*if (history_queue_.find(frame_id) == history_queue_.end() && cache_queue_.find(frame_id) == cache_queue_.end()) {
    return;
  }
  if (evictable_.find(frame_id) == evictable_.end()) {
    return;
  }*/
  history_queue_.erase(frame_id);
  cache_queue_.erase(frame_id);
  evictable_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return evictable_.size();
}

}  // namespace bustub

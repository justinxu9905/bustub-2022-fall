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
  history_queue_ = std::list<std::pair<frame_id_t, int>>();
  cache_queue_ = std::list<std::pair<frame_id_t, int>>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (history_queue_.empty() && cache_queue_.empty()) {
    return false;
  }
  for (auto kv_pair : history_queue_) {
    if (evictable_.find(kv_pair.first) != evictable_.end()) {
      *frame_id = kv_pair.first;
    }
  }
  if (history_map_.find(*frame_id) != history_map_.end()) {
    history_queue_.erase(history_map_[*frame_id]);
    history_map_.erase(*frame_id);
    evictable_.erase(*frame_id);
    if (cache_map_.find(*frame_id) != cache_map_.end()) {
      cache_queue_.erase(cache_map_[*frame_id]);
      cache_map_.erase(*frame_id);
    }
    return true;
  }
  for (auto kv_pair : cache_queue_) {
    if (evictable_.find(kv_pair.first) != evictable_.end()) {
      *frame_id = kv_pair.first;
    }
  }
  if (cache_map_.find(*frame_id) != cache_map_.end()) {
    cache_queue_.erase(cache_map_[*frame_id]);
    cache_map_.erase(*frame_id);
    evictable_.erase(*frame_id);
    if (history_map_.find(*frame_id) != history_map_.end()) {
      history_queue_.erase(history_map_[*frame_id]);
      history_map_.erase(*frame_id);
    }
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  int access_cnt = 0;
  if (history_map_.find(frame_id) != history_map_.end()) {
    access_cnt = history_map_[frame_id]->second;
    history_queue_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);
  }
  history_queue_.push_front({frame_id, access_cnt + 1});
  history_map_[frame_id] = history_queue_.begin();

  if (history_map_[frame_id]->second >= static_cast<int>(k_)) {
    history_map_.erase(frame_id);
    history_queue_.pop_front();
    if (cache_map_.find(frame_id) != cache_map_.end()) {
      cache_queue_.erase(cache_map_[frame_id]);
      cache_map_.erase(frame_id);
    }
    cache_queue_.push_front({frame_id, access_cnt + 1});
    cache_map_[frame_id] = cache_queue_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (set_evictable) {
    evictable_[frame_id] = true;
  } else {
    evictable_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (history_map_.find(frame_id) != history_map_.end()) {
    history_queue_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);
  }
  if (cache_map_.find(frame_id) != cache_map_.end()) {
    cache_queue_.erase(cache_map_[frame_id]);
    cache_map_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t { return evictable_.size(); }

}  // namespace bustub

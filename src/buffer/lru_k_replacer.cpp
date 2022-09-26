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
  history_queue = std::list<std::pair<frame_id_t, int>>();
  cache_queue = std::list<std::pair<frame_id_t, int>>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (history_queue.empty() && cache_queue.empty()) {
    return false;
  }
  for (auto kv_pair: history_queue) {
    if (evictable.find(kv_pair.first) != evictable.end()) {
      *frame_id = kv_pair.first;
    }
  }
  if (history_map.find(*frame_id) != history_map.end()) {
    history_queue.erase(history_map[*frame_id]);
    history_map.erase(*frame_id);
    evictable.erase(*frame_id);
    if (cache_map.find(*frame_id) != cache_map.end()) {
      cache_queue.erase(cache_map[*frame_id]);
      cache_map.erase(*frame_id);
    }
    return true;
  }
  for (auto kv_pair: cache_queue) {
    if (evictable.find(kv_pair.first) != evictable.end()) {
      *frame_id = kv_pair.first;
    }
  }
  if (cache_map.find(*frame_id) != cache_map.end()) {
    cache_queue.erase(cache_map[*frame_id]);
    cache_map.erase(*frame_id);
    evictable.erase(*frame_id);
    if (history_map.find(*frame_id) != history_map.end()) {
      history_queue.erase(history_map[*frame_id]);
      history_map.erase(*frame_id);
    }
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  int access_cnt = 0;
  if (history_map.find(frame_id) != history_map.end()) {
    access_cnt = history_map[frame_id]->second;
    history_queue.erase(history_map[frame_id]);
    history_map.erase(frame_id);
  }
  history_queue.push_front({frame_id, access_cnt + 1});
  history_map[frame_id] = history_queue.begin();

  if (history_map[frame_id]->second >= int(k_)) {
    history_map.erase(frame_id);
    history_queue.pop_front();
    if (cache_map.find(frame_id) != cache_map.end()) {
      cache_queue.erase(cache_map[frame_id]);
      cache_map.erase(frame_id);
    }
    cache_queue.push_front({frame_id, access_cnt + 1});
    cache_map[frame_id] = cache_queue.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (set_evictable) {
    evictable[frame_id] = true;
  } else {
    evictable.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (history_map.find(frame_id) != history_map.end()) {
    history_queue.erase(history_map[frame_id]);
  }
  if (cache_map.find(frame_id) != cache_map.end()) {
    cache_queue.erase(cache_map[frame_id]);
  }
}

auto LRUKReplacer::Size() -> size_t {
  return evictable.size();
}

}  // namespace bustub

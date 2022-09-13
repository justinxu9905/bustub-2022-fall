//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  head = new node(0, NULL, NULL);
  tail = new node(0, NULL, NULL);
  head->next = tail;
  tail->prev = head;
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (frame_map.empty()) {
    *frame_id = frame_id_t(0);
    return false;
  }

  replacer_lock.lock();

  node *frame_node = tail->prev;
  *frame_id = frame_node->key;
  tail->prev = frame_node->prev;
  frame_node->prev->next = tail;

  frame_map.erase(frame_node->key);

  free(frame_node);

  replacer_lock.unlock();

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (frame_map.count(frame_id) != 0U) {
    replacer_lock.lock();

    node *frame_node = frame_map[frame_id];
    frame_node->prev->next = frame_node->next;
    frame_node->next->prev = frame_node->prev;
    free(frame_node);

    frame_map.erase(frame_id);

    replacer_lock.unlock();
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frame_map.count(frame_id) == 0U) {
    replacer_lock.lock();

    node *frame_node = new node(frame_id, head, head->next);
    head->next->prev = frame_node;
    head->next = frame_node;

    frame_map.insert({frame_id, frame_node});

    replacer_lock.unlock();
  }
}

auto LRUReplacer::Size() -> size_t {
  replacer_lock.lock();

  size_t size = frame_map.size();

  replacer_lock.unlock();

  return size;
}

}  // namespace bustub

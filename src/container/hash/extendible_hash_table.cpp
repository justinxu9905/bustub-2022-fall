//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  global_depth_ = 0;
  num_buckets_ = 1;
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[idx];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[idx];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[idx];
  V val;
  if (bucket->Find(key, val)) {
    if (val == value) {
      return;
    }
    bucket->Remove(key);
  }
  if (bucket->IsFull()) {
    if (GetLocalDepthInternal(idx) == GetGlobalDepthInternal()) {
      for (int i = 0; i < 1 << global_depth_; i++) {
        dir_.push_back(dir_[i]);
      }
      global_depth_++;
    }
    bucket->IncrementDepth();
    SplitBucket(idx);
    InsertInternal(key, value);
    return;
  }
  bucket->Insert(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  std::list<std::pair<K, V>> items;
  std::copy(bucket->GetItems().begin(), bucket->GetItems().end(), std::back_inserter(items));
  bucket->GetItems().clear();
  std::pair<K, V> item;

  int n = items.size();
  for (int i = 0; i < n; i++) {
    item = items.front();
    InsertInternal(item.first, item.second);
    items.pop_front();
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &elem : list_) {
    if (elem.first == key) {
      value = elem.second;
      return true;
    }
  }
  value = {};
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  V val;
  if (Find(key, val)) {
    list_.remove(std::make_pair(key, val));
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto elem = list_.begin(); elem != list_.end(); elem++) {
    if (elem->first == key) {
      elem->second = value;
    }
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

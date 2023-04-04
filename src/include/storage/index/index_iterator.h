//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(Page *page_ptr, BufferPoolManager *buffer_pool_manager, int index = 0);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return this->page_id_ == itr.page_id_ && this->in_page_index_ == itr.in_page_index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return this->page_id_ != itr.page_id_ || this->in_page_index_ != itr.in_page_index_;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  page_id_t page_id_;
  Page *page_ptr_;
  int in_page_index_;
};

}  // namespace bustub

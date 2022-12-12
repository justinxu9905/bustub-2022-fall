//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ItemAt(int index) -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int size = GetSize();

  int left = 0;
  int right = size - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int ret = comparator(key, array_[mid].first);

    if (ret == 1) {
      left = mid + 1;
    } else if (ret == -1) {
      right = mid - 1;
    } else {
      return mid;
    }
  }

  return left;
}

/*
 * Insert a key-value pair in the leaf page
 * return the size after insertion
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int size = GetSize();

  if (size == 0) {
    InsertAt(0, key, value);
    return 1;
  }

  int index = KeyIndex(key, comparator);
  if (comparator(key, KeyAt(index)) == 0) {
    return size;
  }

  InsertAt(index, key, value);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) {
  int size = GetSize();

  for (int i = size - 1; i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index] = MappingType{key, value};

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  int size = GetSize();

  for (int i = index; i < size; i++) {
    array_[i] = array_[i + 1];
  }

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstItemToBackOf(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node) {
  int size = GetSize();
  if (size == 0) {
    return;
  }

  leaf_node->InsertAt(leaf_node->GetSize(), KeyAt(0), ValueAt(0));
  RemoveAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastItemToFrontOf(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node) {
  int size = GetSize();
  if (size == 0) {
    return;
  }

  leaf_node->InsertAt(0, KeyAt(size - 1), ValueAt(size - 1));
  RemoveAt(size - 1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

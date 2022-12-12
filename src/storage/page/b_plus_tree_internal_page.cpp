//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  int size = GetSize();

  for (int index = 0; index < size; ++index) {  // size: number of children/kv pair
    if (ValueAt(index) == value) {
      return index;
    }
  }

  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int size = GetSize();

  int index = GetSize();
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(key, KeyAt(i)) < 0) {
      index = i;
      break;
    }
  }

  if (comparator(key, KeyAt(index)) == 0) {
    return size;
  }

  InsertAt(index, key, value);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) {
  // std::cout << "[Internal InsertAt] index: " << index << " key: " << key << std::endl;
  int size = GetSize();

  for (int i = size - 1; i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index] = MappingType{key, value};

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) {
  int size = GetSize();

  for (int i = index; i < size; i++) {
    array_[i] = array_[i + 1];
  }

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookupIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  for (int i = 1; i < GetSize(); i++) {
    if (comparator(key, array_[i].first) < 0) {
      return i - 1;
    }
  }
  return GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  return ValueAt(LookupIndex(key, comparator));
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstItemToBackOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_node,
                                                           BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  if (size == 0) {
    return;
  }

  page_id_t moved_page_id = ValueAt(0);
  Page *moved_page_ptr = buffer_pool_manager->FetchPage(moved_page_id);
  auto *moved_node = reinterpret_cast<BPlusTreePage *>(moved_page_ptr->GetData());
  moved_node->SetParentPageId(internal_node->GetPageId());

  internal_node->InsertAt(internal_node->GetSize(), KeyAt(0), ValueAt(0));
  RemoveAt(0);

  buffer_pool_manager->UnpinPage(moved_page_ptr->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastItemToFrontOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *internal_node,
                                                           BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  if (size == 0) {
    return;
  }

  page_id_t moved_page_id = ValueAt(size - 1);
  Page *moved_page_ptr = buffer_pool_manager->FetchPage(moved_page_id);
  auto *moved_node = reinterpret_cast<BPlusTreePage *>(moved_page_ptr->GetData());
  moved_node->SetParentPageId(internal_node->GetPageId());

  internal_node->InsertAt(0, KeyAt(size - 1), ValueAt(size - 1));
  RemoveAt(size - 1);

  buffer_pool_manager->UnpinPage(moved_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BuildRoot(const KeyType &old_key, const ValueType &old_value,
                                               const KeyType &new_key, const ValueType &new_value) {
  SetSize(2);
  SetKeyAt(0, old_key);
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

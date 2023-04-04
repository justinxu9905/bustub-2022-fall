/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page_ptr, BufferPoolManager *buffer_pool_manager, int index)
    : buffer_pool_manager_(buffer_pool_manager), page_ptr_(page_ptr), in_page_index_(index) {
  if (page_ptr_ == nullptr) {
    page_id_ = INVALID_PAGE_ID;
  } else {
    page_id_ = page_ptr_->GetPageId();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (!IsEnd()) {
    page_ptr_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(page_ptr_ != nullptr);
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_ptr_->GetData())->ItemAt(in_page_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }

  auto *leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_ptr_->GetData());

  if (in_page_index_ == leaf_node->GetSize() - 1) {
    page_id_t old_page_id = page_id_;
    Page *old_page_ptr = page_ptr_;

    page_id_ = leaf_node->GetNextPageId();
    in_page_index_ = 0;
    if (page_id_ == INVALID_PAGE_ID) {
      page_ptr_ = nullptr;
    } else {
      page_ptr_ = buffer_pool_manager_->FetchPage(page_id_);
      page_ptr_->RLatch();
      /* Handle deadlock
       * if (!page_ptr_->TryRLatch()) {
        old_page_ptr->RUnlatch();
        buffer_pool_manager_->UnpinPage(old_page_id, false);

        page_id_ = INVALID_PAGE_ID;
        in_page_index_ = 0;
        page_ptr_ = nullptr;
        return *this;
      }*/
    }

    old_page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(old_page_id, false);
  } else {
    in_page_index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

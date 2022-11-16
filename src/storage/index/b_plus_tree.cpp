#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  std::cout << "[GetValue] key: " << key << std::endl;
  root_latch_.RLock();
  std::cout << "root_latch RLatch" << std::endl;
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(nullptr);  // means root of b+ tree locked
  }

  if (IsEmpty()) {
    if (transaction != nullptr) {
      ReleaseQueuedLatches(transaction, READ_MODE);
    } else {
      root_latch_.RUnlock();
    }
    return false;
  }

  Page *page_ptr = FindLeaf(key, READ_MODE, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  if (comparator_(key, leaf_node->KeyAt(index)) == 0) {
    result->push_back(leaf_node->ValueAt(index));

    if (transaction != nullptr) {
      ReleaseQueuedLatches(transaction, READ_MODE);
    } else {
      std::cout << "page " << page_ptr->GetPageId() << " RUnlatch" << std::endl;
      page_ptr->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    }
    return true;
  }

  if (transaction != nullptr) {
    ReleaseQueuedLatches(transaction, READ_MODE);
  } else {
    std::cout << "page " << page_ptr->GetPageId() << " RUnlatch" << std::endl;
    page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  assert(transaction != nullptr);  // transaction should not be null for insert operations
  std::cout << "[Insert] key: " << key << std::endl;
  root_latch_.WLock();
  std::cout << "t_id: " << transaction->GetThreadId() << " root_latch WLatch" << std::endl;
  transaction->AddIntoPageSet(nullptr);  // means root of b+ tree locked

  if (IsEmpty()) {
    Page *page_ptr = buffer_pool_manager_->NewPage(&root_page_id_);

    UpdateRootPageId(1);

    auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());
    leaf_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf_node->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    Page *page_ptr = FindLeaf(key, INSERT_MODE, transaction);
    auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());

    int old_size = leaf_node->GetSize();
    int new_size = leaf_node->Insert(key, value, comparator_);

    // Handle splitting
    if (new_size == leaf_node->GetMaxSize()) {
      auto *split_page_ptr = Split<LeafPage>(leaf_node, transaction);
      auto *split_node = reinterpret_cast<LeafPage *>(split_page_ptr->GetData());

      // update next and prev page ids
      split_node->SetNextPageId(leaf_node->GetNextPageId());
      leaf_node->SetNextPageId(split_node->GetPageId());
      split_node->SetPrevPageId(leaf_node->GetPageId());
      if (split_node->GetNextPageId() != INVALID_PAGE_ID) {
        Page *next_page_ptr = buffer_pool_manager_->FetchPage(split_node->GetNextPageId());
        auto *next_leaf_node = reinterpret_cast<LeafPage *>(next_page_ptr->GetData());

        next_leaf_node->SetPrevPageId(split_node->GetPageId());

        buffer_pool_manager_->UnpinPage(next_page_ptr->GetPageId(), true);
      }

      // move elements to the new leaf node
      while (split_node->GetSize() < split_node->GetMinSize()) {
        leaf_node->MoveLastItemToFrontOf(split_node);
      }

      // Generate new root if needed
      if (leaf_node->IsRootPage()) {
        Page *root_page_ptr = buffer_pool_manager_->NewPage(&root_page_id_);

        UpdateRootPageId(0);

        auto *root_node = reinterpret_cast<InternalPage *>(root_page_ptr->GetData());
        root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
        // parent_node->InsertAt(0, leaf_node->KeyAt(0), leaf_node->GetPageId());
        root_node->BuildRoot(leaf_node->KeyAt(0), leaf_node->GetPageId(), split_node->KeyAt(0),
                             split_node->GetPageId());
        leaf_node->SetParentPageId(root_node->GetPageId());
        split_node->SetParentPageId(root_node->GetPageId());

        buffer_pool_manager_->UnpinPage(root_page_id_, true);
      } else {
        InsertIntoParent(leaf_node->KeyAt(0), leaf_node, split_node->KeyAt(0), split_node, transaction);
      }

      buffer_pool_manager_->UnpinPage(split_page_ptr->GetPageId(), true);
    }

    // buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);

    if (old_size == new_size) {
      ReleaseQueuedLatches(transaction, INSERT_MODE);
      return false;
    }
  }

  ReleaseQueuedLatches(transaction, INSERT_MODE);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  assert(transaction != nullptr);  // transaction should not be null for remove operations
  std::cout << "[Remove] key: " << key << std::endl;
  root_latch_.WLock();
  std::cout << "t_id: " << transaction->GetThreadId() << " root_latch WLatch" << std::endl;
  transaction->AddIntoPageSet(nullptr);

  if (IsEmpty()) {
    ReleaseQueuedLatches(transaction, REMOVE_MODE);
    return;
  }

  Page *page_ptr = FindLeaf(key, REMOVE_MODE, transaction);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);

  if (comparator_(key, leaf_node->KeyAt(index)) != 0) {
    ReleaseQueuedLatches(transaction, REMOVE_MODE);
    return;
  }

  leaf_node->RemoveAt(index);

  // update key in parent if needed
  if (index == 0 && leaf_node->GetSize() && !leaf_node->IsRootPage()) {
    page_id_t parent_page_id = leaf_node->GetParentPageId();
    auto *parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));

    parent_node->SetKeyAt(parent_node->ValueIndex(leaf_node->GetPageId()), leaf_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }

  // redistribute or coalesce if needed
  if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
    RedistributeOrCoalesce(leaf_node, transaction);
  }

  // remove the node and update prev and next pages' links
  if (leaf_node->GetSize() == 0) {
    transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());

    if (leaf_node->GetPrevPageId() != INVALID_PAGE_ID) {
      Page *prev_page_ptr = buffer_pool_manager_->FetchPage(leaf_node->GetPrevPageId());
      auto *prev_leaf_node = reinterpret_cast<LeafPage *>(prev_page_ptr->GetData());

      prev_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
      buffer_pool_manager_->UnpinPage(prev_page_ptr->GetPageId(), true);
    }

    if (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
      Page *next_page_ptr = buffer_pool_manager_->FetchPage(leaf_node->GetNextPageId());
      auto *next_leaf_node = reinterpret_cast<LeafPage *>(next_page_ptr->GetData());

      next_leaf_node->SetPrevPageId(leaf_node->GetPrevPageId());
      buffer_pool_manager_->UnpinPage(next_page_ptr->GetPageId(), true);
    }
  }

  CleanupDeletedPages(transaction);
  ReleaseQueuedLatches(transaction, REMOVE_MODE);
  // buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, int latch_mode, Transaction *transaction) -> Page * {
  assert(transaction != nullptr ||
         (latch_mode != INSERT_MODE &&
          latch_mode != REMOVE_MODE));  // transaction should not be null for insert and remove operations
  // std::cout << "[FindLeaf] key: " << key << std::endl;
  page_id_t page_id = root_page_id_;
  Page *page_ptr = nullptr;
  Page *prev_page_ptr = nullptr;
  while (true) {
    prev_page_ptr = page_ptr;
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    if (page_ptr == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "fail to fetch page");
    }
    auto *tree_page_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());

    // latch crabbing
    switch (latch_mode) {
      case READ_MODE:
        std::cout << "page " << page_ptr->GetPageId() << " RLatch" << std::endl;
        page_ptr->RLatch();
        if (prev_page_ptr != nullptr) {
          std::cout << "page " << prev_page_ptr->GetPageId() << " RUnlatch" << std::endl;
          prev_page_ptr->RUnlatch();
          buffer_pool_manager_->UnpinPage(prev_page_ptr->GetPageId(), false);
        } else {
          std::cout << "root_latch RUnlatch" << std::endl;
          root_latch_.RUnlock();
        }
        if (transaction != nullptr) {
          transaction->AddIntoPageSet(page_ptr);
        }
        break;
      case INSERT_MODE:
        page_ptr->WLatch();
        std::cout << "t_id: " << transaction->GetThreadId() << " page " << page_ptr->GetPageId() << " WLatch"
                  << std::endl;
        if (tree_page_ptr->GetSize() < tree_page_ptr->GetMaxSize() - 1 ||
            (!tree_page_ptr->IsLeafPage() && tree_page_ptr->GetSize() == tree_page_ptr->GetMaxSize() - 1)) {
          ReleaseQueuedLatches(transaction, latch_mode);
        }
        transaction->AddIntoPageSet(page_ptr);
        break;
      case REMOVE_MODE:
        page_ptr->WLatch();
        std::cout << "t_id: " << transaction->GetThreadId() << " page " << page_ptr->GetPageId() << " WLatch"
                  << std::endl;
        if (tree_page_ptr->GetSize() > tree_page_ptr->GetMinSize()) {
          ReleaseQueuedLatches(transaction, latch_mode);
        }
        transaction->AddIntoPageSet(page_ptr);
        break;
    }

    if (tree_page_ptr->IsLeafPage()) {
      std::cout << "leaf: " << page_ptr->GetPageId() << " for key: " << key << std::endl;
      return page_ptr;
    }

    std::cout << "internal: " << page_ptr->GetPageId() << " for key: " << key << std::endl;

    auto *internal_node = reinterpret_cast<InternalPage *>(tree_page_ptr);
    page_id = internal_node->Lookup(key, comparator_);
    // buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseQueuedLatches(Transaction *transaction, int latch_mode) {
  if (transaction == nullptr) {
    return;
  }
  // std::cout << "[ReleaseQueuedLatches] t_id: " << transaction->GetThreadId() << std::endl;

  std::shared_ptr<std::deque<Page *>> queue = transaction->GetPageSet();
  while (!queue->empty()) {
    Page *page_ptr = queue->front();
    queue->pop_front();

    if (page_ptr != nullptr) {
      switch (latch_mode) {
        case READ_MODE:
          std::cout << "t_id: " << transaction->GetThreadId() << " page " << page_ptr->GetPageId() << " RUnlatch"
                    << std::endl;
          page_ptr->RUnlatch();
          buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
          break;
        case INSERT_MODE:
        case REMOVE_MODE:
          std::cout << "t_id: " << transaction->GetThreadId() << " page " << page_ptr->GetPageId() << " WUnlatch"
                    << std::endl;
          page_ptr->WUnlatch();
          buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
          break;
      }
    } else {
      switch (latch_mode) {
        case READ_MODE:
          std::cout << "t_id: " << transaction->GetThreadId() << "root_latch RUnlatch" << std::endl;
          root_latch_.RUnlock();
          break;
        case INSERT_MODE:
        case REMOVE_MODE:
          std::cout << "t_id: " << transaction->GetThreadId() << " root_latch WUnlatch" << std::endl;
          root_latch_.WUnlock();
          break;
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *old_node, Transaction *transaction) -> Page * {
  page_id_t page_id;
  Page *new_page_ptr = buffer_pool_manager_->NewPage(&page_id);
  // std::cout << "[Split] old_node: " << old_node->GetPageId() << " new_node: " << new_page_ptr->GetPageId() <<
  // std::endl;

  // create a new leaf node
  auto *new_node = reinterpret_cast<N *>(new_page_ptr->GetData());
  new_node->Init(page_id, INVALID_PAGE_ID, old_node->GetMaxSize());

  return new_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(const KeyType &old_key, BPlusTreePage *old_node, const KeyType &new_key,
                                      BPlusTreePage *new_node, Transaction *transaction) -> bool {
  // std::cout << "[InsertIntoParent] key: " << key << " new_node: " << new_node->GetPageId() << " parent: " <<
  // old_node->GetParentPageId() << std::endl;
  page_id_t parent_page_id = old_node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    // std::cout << "invalid" << std::endl;
    return false;
  }
  auto *parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));

  // update old_node's key: it is possible that the first item of old_node has changed
  parent_node->SetKeyAt(parent_node->ValueIndex(old_node->GetPageId()), old_key);

  parent_node->Insert(new_key, new_node->GetPageId(), comparator_);
  new_node->SetParentPageId(parent_page_id);

  // Handle splitting
  if (parent_node->GetSize() > parent_node->GetMaxSize()) {
    auto *split_page_ptr = Split<InternalPage>(parent_node, transaction);
    auto *split_node = reinterpret_cast<InternalPage *>(split_page_ptr->GetData());

    // move elements to the new leaf node
    while (split_node->GetSize() < split_node->GetMinSize()) {
      parent_node->MoveLastItemToFrontOf(split_node, buffer_pool_manager_);
    }

    // Generate new root if needed
    if (parent_node->IsRootPage()) {
      Page *root_page_ptr = buffer_pool_manager_->NewPage(&root_page_id_);

      UpdateRootPageId(0);

      auto *root_node = reinterpret_cast<InternalPage *>(root_page_ptr->GetData());
      root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      // new_parent_node->InsertAt(1, parent_node->KeyAt(0), parent_node->GetPageId());
      root_node->BuildRoot(parent_node->KeyAt(0), parent_node->GetPageId(), split_node->KeyAt(0),
                           split_node->GetPageId());
      parent_node->SetParentPageId(root_node->GetPageId());
      split_node->SetParentPageId(root_node->GetPageId());

      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    } else {
      InsertIntoParent(parent_node->KeyAt(0), parent_node, split_node->KeyAt(0), split_node, transaction);
    }

    buffer_pool_manager_->UnpinPage(split_page_ptr->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::RedistributeOrCoalesce(N *node, Transaction *transaction) {
  // TODO(xuzixiang): optimize page fetch and unpin
  // std::cout << "[RedistributeOrCoalesce] node: " << node->GetPageId() << std::endl;
  if (node->IsRootPage()) {
    if (node->GetSize() > 1) {
      return;
    }
    if (node->IsLeafPage()) {
      if (node->GetSize() == 1) {
        return;
      }
      root_page_id_ = INVALID_PAGE_ID;
    } else {
      auto *old_root = reinterpret_cast<InternalPage *>(node);
      root_page_id_ = old_root->ValueAt(0);
      auto *new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
      new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
    return;
  }
  page_id_t parent_page_id = node->GetParentPageId();
  auto *parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));

  int node_index = parent_node->ValueIndex(node->GetPageId());
  if (node_index > 0) {  // not the first child
    page_id_t prev_page_id = parent_node->ValueAt(node_index - 1);
    auto *prev_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(prev_page_id));

    // if the neighbor got enough children, then redistribute
    if (prev_node->GetSize() > prev_node->GetMinSize()) {
      Redistribute(prev_node, node, true);

      parent_node->SetKeyAt(node_index, node->KeyAt(0));

      buffer_pool_manager_->UnpinPage(prev_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      return;
    }

    buffer_pool_manager_->UnpinPage(prev_page_id, false);
  }
  if (node_index < parent_node->GetSize() - 1) {  // not the last child
    page_id_t next_page_id = parent_node->ValueAt(node_index + 1);
    auto *next_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(next_page_id));

    // if the neighbor got enough children, then redistribute
    if (next_node->GetSize() > next_node->GetMinSize()) {
      Redistribute(next_node, node, false);

      parent_node->SetKeyAt(node_index + 1, next_node->KeyAt(0));

      buffer_pool_manager_->UnpinPage(next_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      return;
    }

    buffer_pool_manager_->UnpinPage(next_page_id, false);
  }

  // try to coalesce
  if (node_index > 0) {  // not the first child
    page_id_t prev_page_id = parent_node->ValueAt(node_index - 1);
    auto *prev_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(prev_page_id));

    if (prev_node->GetSize() == prev_node->GetMinSize()) {
      Coalesce(prev_node, node, true);

      parent_node->RemoveAt(node_index);
      std::cout << "node: " << node->GetPageId() << " got removed from parent" << std::endl;

      transaction->AddIntoDeletedPageSet(node->GetPageId());
      buffer_pool_manager_->UnpinPage(prev_page_id, true);
    }
  } else if (node_index < parent_node->GetSize() - 1) {  // not the last child
    page_id_t next_page_id = parent_node->ValueAt(node_index + 1);
    auto *next_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(next_page_id));

    if (next_node->GetSize() == next_node->GetMinSize()) {
      Coalesce(next_node, node, false);

      parent_node->SetKeyAt(node_index + 1, next_node->KeyAt(0));
      parent_node->RemoveAt(node_index);
      std::cout << "node: " << node->GetPageId() << " got removed from parent" << std::endl;

      transaction->AddIntoDeletedPageSet(node->GetPageId());
      buffer_pool_manager_->UnpinPage(next_page_id, true);
    }
  }

  if (parent_node->GetSize() < parent_node->GetMinSize()) {
    RedistributeOrCoalesce(parent_node, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, bool from_prev) {
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (from_prev) {
      neighbor_leaf_node->MoveLastItemToFrontOf(leaf_node);
    } else {
      neighbor_leaf_node->MoveFirstItemToBackOf(leaf_node);
    }
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (from_prev) {
      neighbor_internal_node->MoveLastItemToFrontOf(internal_node, buffer_pool_manager_);
    } else {
      neighbor_internal_node->MoveFirstItemToBackOf(internal_node, buffer_pool_manager_);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, bool into_prev) {
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (into_prev) {
      while (leaf_node->GetSize()) {
        leaf_node->MoveFirstItemToBackOf(neighbor_leaf_node);
      }

      // update next and prev page ids
      neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
      if (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
        Page *next_page_ptr = buffer_pool_manager_->FetchPage(leaf_node->GetNextPageId());
        auto *next_leaf_node = reinterpret_cast<LeafPage *>(next_page_ptr->GetData());

        next_leaf_node->SetPrevPageId(neighbor_leaf_node->GetPageId());

        buffer_pool_manager_->UnpinPage(next_page_ptr->GetPageId(), true);
      }
    } else {
      while (leaf_node->GetSize()) {
        leaf_node->MoveLastItemToFrontOf(neighbor_leaf_node);
      }

      // update next and prev page ids
      neighbor_leaf_node->SetPrevPageId(leaf_node->GetPrevPageId());
      if (leaf_node->GetPrevPageId() != INVALID_PAGE_ID) {
        Page *prev_page_ptr = buffer_pool_manager_->FetchPage(leaf_node->GetPrevPageId());
        auto *prev_leaf_node = reinterpret_cast<LeafPage *>(prev_page_ptr->GetData());

        prev_leaf_node->SetNextPageId(neighbor_leaf_node->GetPageId());

        buffer_pool_manager_->UnpinPage(prev_page_ptr->GetPageId(), true);
      }
    }
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (into_prev) {
      while (internal_node->GetSize()) {
        internal_node->MoveFirstItemToBackOf(neighbor_internal_node, buffer_pool_manager_);
      }
    } else {
      while (internal_node->GetSize()) {
        internal_node->MoveLastItemToFrontOf(neighbor_internal_node, buffer_pool_manager_);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CleanupDeletedPages(Transaction *transaction) {
  const auto deleted_pages = transaction->GetDeletedPageSet();
  for (const auto &page_id : *deleted_pages) {
    std::cout << "[CleanupDeletedPages] page_id: " << page_id << std::endl;
    buffer_pool_manager_->DeletePage(page_id);
  }
  deleted_pages->clear();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::cout << "[Begin]" << std::endl;
  root_latch_.RLock();
  std::cout << "root_latch RLatch" << std::endl;
  page_id_t page_id = root_page_id_;
  Page *page_ptr = nullptr;
  Page *prev_page_ptr = nullptr;
  while (true) {
    prev_page_ptr = page_ptr;
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    auto *tree_page_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());

    page_ptr->RLatch();
    std::cout << "page " << page_ptr->GetPageId() << " RLatch" << std::endl;
    if (prev_page_ptr != nullptr) {
      std::cout << "page " << prev_page_ptr->GetPageId() << " RUnlatch" << std::endl;
      prev_page_ptr->RUnlatch();
      buffer_pool_manager_->UnpinPage(prev_page_ptr->GetPageId(), false);
    } else {
      std::cout << "root_latch RUnlatch" << std::endl;
      root_latch_.RUnlock();
    }

    if (tree_page_ptr->IsLeafPage()) {
      return INDEXITERATOR_TYPE(page_ptr, 0, buffer_pool_manager_);
    }

    auto *internal_node = reinterpret_cast<InternalPage *>(tree_page_ptr);
    page_id = internal_node->ValueAt(0);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::cout << "[Begin] key: " << key << std::endl;
  root_latch_.RLock();
  Page *page_ptr = FindLeaf(key, READ_MODE);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(page_ptr, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  // std::cout << "[UpdateRootPageId] new root page id: " << root_page_id_ << std::endl;
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

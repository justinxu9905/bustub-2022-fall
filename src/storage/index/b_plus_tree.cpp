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
  Page *page_ptr = FindLeaf(key);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  if (comparator_(key, leaf_node->KeyAt(index)) == 0) {
    result->push_back(leaf_node->ValueAt(index));

    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    return true;
  }

  buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
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
  // std::cout << "[Insert] key: " << key << std::endl;
  if (IsEmpty()) {
    Page *page_ptr = buffer_pool_manager_->NewPage(&root_page_id_);

    UpdateRootPageId(1);

    auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());
    leaf_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf_node->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    Page *page_ptr = FindLeaf(key);
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
        leaf_node->MoveLastItemToFront(split_node);
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
        InsertIntoParent(leaf_node, split_node->KeyAt(0), split_node, transaction);
      }

      buffer_pool_manager_->UnpinPage(split_page_ptr->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);

    if (old_size == new_size) {
      return false;
    }
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> Page * {
  // std::cout << "[FindLeaf] key: " << key << std::endl;
  page_id_t page_id = root_page_id_;
  while (true) {
    Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
    auto *tree_page_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());

    if (tree_page_ptr->IsLeafPage()) {
      // std::cout << "leaf: " << page_ptr->GetPageId() << std::endl;
      return page_ptr;
    }

    // std::cout << "internal: " << page_ptr->GetPageId() << std::endl;

    auto *internal_node = reinterpret_cast<InternalPage *>(tree_page_ptr);
    page_id = internal_node->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
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
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) -> bool {
  // std::cout << "[InsertIntoParent] key: " << key << " new_node: " << new_node->GetPageId() << " parent: " <<
  // old_node->GetParentPageId() << std::endl;
  page_id_t parent_page_id = old_node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    // std::cout << "invalid" << std::endl;
    return false;
  }
  auto *parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));
  parent_node->Insert(key, new_node->GetPageId(), comparator_);
  new_node->SetParentPageId(parent_page_id);

  // Handle splitting
  if (parent_node->GetSize() == parent_node->GetMaxSize()) {
    auto *split_page_ptr = Split<InternalPage>(parent_node, transaction);
    auto *split_node = reinterpret_cast<InternalPage *>(split_page_ptr->GetData());

    // move elements to the new leaf node
    while (split_node->GetSize() < split_node->GetMinSize()) {
      parent_node->MoveLastItemToFront(split_node, buffer_pool_manager_);
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
      InsertIntoParent(parent_node, split_node->KeyAt(0), split_node, transaction);
    }

    buffer_pool_manager_->UnpinPage(split_page_ptr->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  return false;
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
  if (IsEmpty()) {
    return;
  }
  Page *page_ptr = FindLeaf(key);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  if (comparator_(key, leaf_node->KeyAt(index)) == 0) {
    leaf_node->RemoveAt(index);
  }

  // remove node if empty
  if (leaf_node->GetSize() == 0) {
    Page *next_page_ptr = buffer_pool_manager_->FetchPage(leaf_node->GetNextPageId());
    auto *next_leaf_node = reinterpret_cast<LeafPage *>(next_page_ptr->GetData());

    Page *prev_page_ptr = buffer_pool_manager_->FetchPage(leaf_node->GetPrevPageId());
    auto *prev_leaf_node = reinterpret_cast<LeafPage *>(prev_page_ptr->GetData());

    next_leaf_node->SetPrevPageId(prev_leaf_node->GetPageId());
    prev_leaf_node->SetNextPageId(next_leaf_node->GetPageId());

    buffer_pool_manager_->UnpinPage(next_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(prev_page_ptr->GetPageId(), true);

    Page *parent_page_ptr = buffer_pool_manager_->FetchPage(leaf_node->GetParentPageId());
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page_ptr->GetData());
    parent_node->RemoveAt(parent_node->LookupIndex(key, comparator_));

    buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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

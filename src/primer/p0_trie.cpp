// This is a placeholder file for clang-tidy check.
//
// With this file, we can fire run_clang_tidy.py to check `p0_trie.h`,
// as it will filter out all header files and won't check header-only code.
//
// This file is not part of the submission. All of the modifications should
// be done in `src/include/primer/p0_trie.h`.

#include "primer/p0_trie.h"
#include <unordered_map>

namespace bustub {

TrieNode::TrieNode(char key_char) {
  key_char_ = key_char;
}

TrieNode::TrieNode(TrieNode &&other_trie_node) noexcept {
  for (auto &v : other_trie_node.children_) {
    children_.insert({v.first, std::move(v.second)});
  }
}

bool TrieNode::HasChild(char key_char) const {
  return children_.count(key_char);
}

bool TrieNode::HasChildren() const {
  return !children_.empty();
}

bool TrieNode::IsEndNode() const {
  return is_end_;
}

char TrieNode::GetKeyChar() const {
  return key_char_;
}

std::unique_ptr<TrieNode> * TrieNode::InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
  if (children_.count(key_char) != 0) {
    return nullptr;
  }
  if (key_char != child->GetKeyChar()) {
    return nullptr;
  }
  children_.insert({key_char, std::move(child)});
  return &children_[key_char];
}

std::unique_ptr<TrieNode> * TrieNode::GetChildNode(char key_char) {
  if (children_.count(key_char) == 0) {
    return nullptr;
  }
  return &children_[key_char];
}

void TrieNode::RemoveChildNode(char key_char) {
  children_.erase(key_char);
}

void TrieNode::SetEndNode(bool is_end) {
  is_end_ = is_end;
}
}

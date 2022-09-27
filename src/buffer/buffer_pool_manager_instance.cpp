//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  auto *frame_id = new frame_id_t;
  if (free_list_.empty()) {
    if (!replacer_->Evict(frame_id)) {
      return nullptr;
    }
    FlushPgImp(pages_[*frame_id].GetPageId());
  }
  *frame_id = free_list_.front();
  free_list_.pop_front();

  Page *new_page = new Page();
  new_page->page_id_ = AllocatePage();
  new_page->is_dirty_ = false;
  new_page->pin_count_ = 1;

  *page_id = new_page->page_id_;

  memcpy(&pages_[*frame_id], new_page, sizeof(Page));
  delete(new_page);

  page_table_->Insert(*page_id, *frame_id);
  replacer_->RecordAccess(*frame_id);
  replacer_->SetEvictable(*frame_id, false);

  return &pages_[*frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // return if found in buffer pool
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() == page_id) {
      pages_[i].pin_count_++;
      return &pages_[i];
    }
  }

  // try to vacate for the new disk fetched page
  auto *frame_id = new frame_id_t;
  if (free_list_.empty()) {
    if (!replacer_->Evict(frame_id)) {
      return nullptr;
    }
    FlushPgImp(pages_[*frame_id].GetPageId());
  }
  *frame_id = free_list_.front();
  free_list_.pop_front();

  // fetch page from disk
  char page_data[PAGE_SIZE];
  disk_manager_->ReadPage(page_id, page_data);
  Page *new_page = new Page();
  new_page->page_id_ = page_id;
  new_page->is_dirty_ = false;
  new_page->pin_count_ = 1;
  memcpy(new_page->data_, page_data, PAGE_SIZE);

  // store it in buffer pool

  memcpy(&pages_[*frame_id], new_page, sizeof(Page));
  delete(new_page);

  page_table_->Insert(page_id, *frame_id);
  replacer_->RecordAccess(*frame_id);
  replacer_->SetEvictable(*frame_id, false);

  return &pages_[*frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  // find page from buffer pool
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  }

  memset(&pages_[frame_id], 0, sizeof(Page));
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  Page page;
  for (frame_id_t i = 0; i < static_cast<int>(pool_size_); i++) {
    if (pages_[i].is_dirty_) {
      disk_manager_->WritePage(i, pages_[i].data_);
    }

    free_list_.emplace_back(i);
    replacer_->Remove(i);
    page_table_->Remove(pages_[i].GetPageId());

    memset(&pages_[i], 0, sizeof(Page));
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

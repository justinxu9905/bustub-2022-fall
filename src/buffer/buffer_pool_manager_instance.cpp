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
  std::cout << "new bmp with pool_size_ " << pool_size_ << std::endl;
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::cout << "[NewPgImp]" << std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    FlushPgInternal(pages_[frame_id].GetPageId());
  }
  frame_id = free_list_.front();
  free_list_.pop_front();

  pages_[frame_id].WLatch();

  pages_[frame_id].page_id_ = AllocatePage();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  *page_id = pages_[frame_id].page_id_;

  pages_[frame_id].WUnlatch();

  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::cout << "[FetchPgImp] " << page_id << std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  // return if found in buffer pool
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() == page_id) {
      pages_[i].WLatch();
      pages_[i].pin_count_++;
      pages_[i].WUnlatch();

      frame_id_t frame_id;
      page_table_->Find(page_id, frame_id);
      replacer_->SetEvictable(frame_id, false);
      return &pages_[i];
    }
  }

  // try to vacate for the new disk fetched page
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    FlushPgInternal(pages_[frame_id].GetPageId());
  }
  frame_id = free_list_.front();
  free_list_.pop_front();

  // fetch page from disk
  char page_data[PAGE_SIZE];
  disk_manager_->ReadPage(page_id, page_data);

  pages_[frame_id].WLatch();

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  memcpy(pages_[frame_id].data_, page_data, PAGE_SIZE);

  pages_[frame_id].WUnlatch();

  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::cout << "[UnpinPgImp] page_id " << page_id << is_dirty << std::endl;
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  pages_[frame_id].WLatch();

  // find page from buffer pool
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  pages_[frame_id].WUnlatch();

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::cout << "[FlushPgImp] page_id " << page_id << std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  return FlushPgInternal(page_id);
}

auto BufferPoolManagerInstance::FlushPgInternal(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  pages_[frame_id].WLatch();

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  }

  pages_[frame_id].ResetMemory();

  pages_[frame_id].WUnlatch();

  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::cout << "[FlushAllPgsImp]" << std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  Page page;
  for (frame_id_t i = 0; i < static_cast<int>(pool_size_); i++) {
    if (pages_[i].is_dirty_) {
      disk_manager_->WritePage(i, pages_[i].data_);
    }

    free_list_.emplace_back(i);
    replacer_->Remove(i);
    page_table_->Remove(pages_[i].GetPageId());

    pages_[i].ResetMemory();
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::cout << "[DeletePgImp] page_id " << page_id << std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  pages_[frame_id].WLatch();

  if (pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].WUnlatch();
    return false;
  }

  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);

  pages_[frame_id].ResetMemory();

  pages_[frame_id].WUnlatch();

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

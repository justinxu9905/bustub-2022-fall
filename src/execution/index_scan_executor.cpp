//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto *tree = reinterpret_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  rids_.clear();
  for (BPlusTreeIndexIteratorForOneIntegerColumn iter = tree->GetBeginIterator(); !iter.IsEnd(); ++iter) {
    rids_.push_back((*iter).second);
  }
  iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == rids_.end()) {
    return false;
  }

  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  Transaction *tx = GetExecutorContext()->GetTransaction();

  *rid = *iter_;
  table_info->table_->GetTuple(*rid, tuple, tx);
  ++iter_;

  return true;
}

}  // namespace bustub

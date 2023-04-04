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
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);
  auto *tree = reinterpret_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());

  rids_.clear();
  if (GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = GetExecutorContext()->GetLockManager()->LockTable(
          GetExecutorContext()->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
          GetExecutorContext()->GetCatalog()->GetTable(index_info->table_name_)->oid_);
      if (!is_locked) {
        throw ExecutionException("IndexScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("IndexScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
  if (plan_->filter_predicate_ != nullptr) {
    const auto *right_expr =
        dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Value v = right_expr->val_;
    tree->ScanKey(Tuple{{v}, index_info->index_->GetKeySchema()}, &rids_, GetExecutorContext()->GetTransaction());
  } else {
    for (BPlusTreeIndexIteratorForOneIntegerColumn iter = tree->GetBeginIterator(); !iter.IsEnd(); ++iter) {
      rids_.push_back((*iter).second);
    }
  }
  rids_iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (rids_iter_ == rids_.end()) {
    return false;
  }

  IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable(index_info->table_name_);
  Transaction *tx = GetExecutorContext()->GetTransaction();

  *rid = *rids_iter_;
  if (GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = GetExecutorContext()->GetLockManager()->LockRow(
          GetExecutorContext()->GetTransaction(), LockManager::LockMode::SHARED, table_info->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("IndexScan Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("IndexScan Executor Get Row Lock Failed");
    }
  }

  table_info->table_->GetTuple(*rid, tuple, tx);
  ++rids_iter_;

  return true;
}

}  // namespace bustub

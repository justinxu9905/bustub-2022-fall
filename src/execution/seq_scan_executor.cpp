//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_({nullptr, RID(), nullptr}) {}

void SeqScanExecutor::Init() {
  // acquire the table lock
  if (GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = GetExecutorContext()->GetLockManager()->LockTable(
          GetExecutorContext()->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
          GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->oid_);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }

  iter_ = GetExecutorContext()
              ->GetCatalog()
              ->GetTable(plan_->GetTableOid())
              ->table_->Begin(GetExecutorContext()->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {
    // release row locks
    if (GetExecutorContext()->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      const auto locked_row_set = GetExecutorContext()->GetTransaction()->GetSharedRowLockSet()->at(
          GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->oid_);
      table_oid_t oid = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->oid_;
      for (auto locked_rid : locked_row_set) {
        GetExecutorContext()->GetLockManager()->UnlockRow(GetExecutorContext()->GetTransaction(), oid, locked_rid);
      }

      GetExecutorContext()->GetLockManager()->UnlockTable(
          GetExecutorContext()->GetTransaction(),
          GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->oid_);
    }
    return false;
  }

  *tuple = *iter_;
  *rid = iter_->GetRid();
  iter_++;

  // acquire the row lock
  if (GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = GetExecutorContext()->GetLockManager()->LockRow(
          GetExecutorContext()->GetTransaction(), LockManager::LockMode::SHARED,
          GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }
  return true;
}

}  // namespace bustub

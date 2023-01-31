//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  printf("[LockTable] %d state: %d isolation level: %d lock mode: %d oid: %d\n", txn->GetTransactionId(),
         static_cast<int>(txn->GetState()), static_cast<int>(txn->GetIsolationLevel()), static_cast<int>(lock_mode),
         oid);

  // make sure txn state, lock mode and isolation level are compatible
  assert(txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED);

  if (txn->GetState() == TransactionState::SHRINKING) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::ABORTED);
          throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        } else {
          txn->SetState(TransactionState::ABORTED);
          throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                                  AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
        break;
    }
  }

  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  // fetch the request queue on this resource
  table_lock_map_latch_.lock();

  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  LockRequestQueue *lock_request_queue = table_lock_map_[oid].get();

  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // TODO(xuzixiang): optimize by using table lock set
  // identify whether there is a lock on this resource
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      // identify whether the lock upgrade is compatible
      switch (lock_request->lock_mode_) {
        case LockMode::EXCLUSIVE:
          lock_request_queue->latch_.unlock();
          txn->SetState(TransactionState::ABORTED);
          throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          break;
        case LockMode::INTENTION_SHARED:
          if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
              lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
            lock_request_queue->latch_.unlock();
            txn->SetState(TransactionState::ABORTED);
            throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          }
          break;
        case LockMode::SHARED:
        case LockMode::INTENTION_EXCLUSIVE:
          if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
            lock_request_queue->latch_.unlock();
            txn->SetState(TransactionState::ABORTED);
            throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lock_mode != LockMode::EXCLUSIVE) {
            lock_request_queue->latch_.unlock();
            txn->SetState(TransactionState::ABORTED);
            throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          }
          break;
      }

      // handle upgrade conflict
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // insert upgrade lock request
      lock_request_queue->request_queue_.remove(lock_request);
      UpdateTableLockSet(txn, lock_request, false);
      delete lock_request;
      auto *upgrade_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);

      std::list<LockRequest *>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      lock_request_queue->latch_.unlock();

      txn->SetState(TransactionState::GROWING);

      // wait to be granted
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          delete upgrade_lock_request;
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      // update lock request and txn lock set
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      UpdateTableLockSet(txn, upgrade_lock_request, true);

      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  // push back new lock request
  auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  lock_request_queue->latch_.unlock();

  txn->SetState(TransactionState::GROWING);

  // wait to be granted
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      delete lock_request;
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  // update lock request and txn lock set
  lock_request->granted_ = true;
  UpdateTableLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  printf("[UnlockTable] %d state: %d isolation level: %d oid: %d\n", txn->GetTransactionId(),
         static_cast<int>(txn->GetState()), static_cast<int>(txn->GetIsolationLevel()), oid);

  // fetch the request queue on this resource
  table_lock_map_latch_.lock();

  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  LockRequestQueue *lock_request_queue = table_lock_map_[oid].get();

  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // TODO(xuzixiang): optimize by using table lock set
  // find the request to be unlocked
  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      // make sure rows are unlocked before unlocking the table
      std::unordered_map<table_oid_t, std::unordered_set<RID>> *s_row_lock_set = txn->GetSharedRowLockSet().get();
      std::unordered_map<table_oid_t, std::unordered_set<RID>> *x_row_lock_set = txn->GetExclusiveRowLockSet().get();

      if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set[oid].empty()) &&
          !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set[oid].empty())) {
        delete lock_request;
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                                AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      }

      // update txn state based on isolation level
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      // update txn lock set
      UpdateTableLockSet(txn, lock_request, false);
      delete lock_request;
      return true;
    }
  }

  // not holding the resource
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

void LockManager::UpdateTableLockSet(Transaction *txn, LockRequest *lock_request, bool insert) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

auto LockManager::GrantLock(LockRequest *lock_request, LockRequestQueue *lock_request_queue) -> bool {
  // make sure the lock request is the first waiting one
  for (auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request != lr) {
      return false;
    } else {
      break;
    }
  }

  return true;
}

}  // namespace bustub

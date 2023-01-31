/**
 * lock_manager_test.cpp
 */

#include "concurrency/lock_manager.h"

#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ((*(txn->GetSharedRowLockSet()))[oid].size(), shared_size);
  EXPECT_EQ((*(txn->GetExclusiveRowLockSet()))[oid].size(), exclusive_size);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  EXPECT_EQ(s_size, txn->GetSharedTableLockSet()->size());
  EXPECT_EQ(x_size, txn->GetExclusiveTableLockSet()->size());
  EXPECT_EQ(is_size, txn->GetIntentionSharedTableLockSet()->size());
  EXPECT_EQ(ix_size, txn->GetIntentionExclusiveTableLockSet()->size());
  EXPECT_EQ(six_size, txn->GetSharedIntentionExclusiveTableLockSet()->size());
}

void TableLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;

  /** 10 tables */
  int num_oids = 10;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an X lock on every table and then unlocks */
  auto task = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, TableLockTest1) { TableLockTest1(); }  // NOLINT

void TableLockTest2() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;

  /** 10 tables */
  int num_oids = 10;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
    EXPECT_EQ(IsolationLevel::REPEATABLE_READ, txns[i]->GetIsolationLevel());
  }

  auto lock_task = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{lock_task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  auto check_lock_task = [&](int txn_id) { CheckTableLockSizes(txns[txn_id], 10, 0, 0, 0, 0); };

  threads.clear();

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{check_lock_task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  auto unlock_task = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  threads.clear();

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{unlock_task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  auto check_unlock_task = [&](int txn_id) { CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0); };

  threads.clear();

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{check_unlock_task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, TableLockTest2) { TableLockTest2(); }

void TableLockTest3() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  Transaction *txn = txn_mgr.Begin();

  /** Take lock */
  EXPECT_EQ(true, lock_mgr.LockTable(txn, LockManager::LockMode::EXCLUSIVE, oid));

  CheckTableLockSizes(txn, 0, 1, 0, 0, 0);

  /** Unlock */
  EXPECT_EQ(true, lock_mgr.UnlockTable(txn, oid));

  /** Clean up */
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  lock_mgr.UnlockTable(txn, oid);

  delete txn;
}
TEST(LockManagerTest, DISABLED_TableLockTest3) { TableLockTest3(); }  // NOLINT

/** Upgrading single transaction from S -> X */
void TableLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();

  /** Take S lock */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid));
  CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);

  /** Upgrade S to X */
  EXPECT_EQ(true, lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid));
  CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);

  /** Clean up */
  txn_mgr.Commit(txn1);
  CheckCommitted(txn1);
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);

  delete txn1;
}
TEST(LockManagerTest, TableLockUpgradeTest1) { TableLockUpgradeTest1(); }  // NOLINT

void TableLockUpgradeTest2() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  std::vector<Transaction *> txns;

  /** 3 tables */
  int num_txns = 3;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Take lock */
  auto lock_task = [&](int txn_id, LockManager::LockMode lock_mode) {
    EXPECT_EQ(true, lock_mgr.LockTable(txns[txn_id], lock_mode, oid));
  };
  std::thread t1 = std::thread{lock_task, 0, LockManager::LockMode::SHARED};
  std::thread t2 = std::thread{lock_task, 1, LockManager::LockMode::SHARED};
  std::thread t3 = std::thread{lock_task, 2, LockManager::LockMode::SHARED};

  t1.join();
  t2.join();
  t3.join();

  std::thread t4 = std::thread{lock_task, 0, LockManager::LockMode::EXCLUSIVE};
  sleep(1);
  CheckTableLockSizes(txns[0], 0, 0, 0, 0, 0);

  /** Unlock */
  EXPECT_EQ(true, lock_mgr.UnlockTable(txns[1], oid));
  CheckTableLockSizes(txns[0], 0, 0, 0, 0, 0);

  EXPECT_EQ(true, lock_mgr.UnlockTable(txns[2], oid));
  t4.join();

  CheckTableLockSizes(txns[0], 0, 1, 0, 0, 0);

  /** Clean up */
  for (int i = 0; i < num_txns; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, TableLockUpgradeTest2) { TableLockUpgradeTest2(); }  // NOLINT

void TableLockBlockTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  std::vector<Transaction *> txns;

  /** 3 tables */
  int num_txns = 3;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Take lock */
  auto lock_task = [&](int txn_id, LockManager::LockMode lock_mode) {
    EXPECT_EQ(true, lock_mgr.LockTable(txns[txn_id], lock_mode, oid));
  };
  std::thread t1 = std::thread{lock_task, 0, LockManager::LockMode::SHARED};
  std::thread t2 = std::thread{lock_task, 1, LockManager::LockMode::INTENTION_SHARED};

  t1.join();
  t2.join();

  CheckTableLockSizes(txns[0], 1, 0, 0, 0, 0);
  CheckTableLockSizes(txns[1], 0, 0, 1, 0, 0);
  CheckTableLockSizes(txns[2], 0, 0, 0, 0, 0);

  std::thread t3 = std::thread{lock_task, 2, LockManager::LockMode::INTENTION_EXCLUSIVE};

  /** Unlock */
  auto unlock_task = [&](int txn_id) { EXPECT_EQ(true, lock_mgr.UnlockTable(txns[txn_id], oid)); };
  std::thread t4 = std::thread{unlock_task, 0};

  t4.join();
  t3.join();

  CheckTableLockSizes(txns[0], 0, 0, 0, 0, 0);
  CheckTableLockSizes(txns[1], 0, 0, 1, 0, 0);
  CheckTableLockSizes(txns[2], 0, 0, 0, 1, 0);

  /** Clean up */
  for (int i = 0; i < num_txns; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, TableLockBlockTest) { TableLockBlockTest(); }  // NOLINT

void RowLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  int num_txns = 3;
  std::vector<Transaction *> txns;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then unlocks */
  auto task = [&](int txn_id) {
    bool res;

    res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);

    res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::SHARED, oid, rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(true, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockRow(txns[txn_id], oid, rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);
    /** Lock set should be updated */
    ASSERT_EQ(false, txns[txn_id]->IsRowSharedLocked(oid, rid));

    res = lock_mgr.UnlockTable(txns[txn_id], oid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}
TEST(LockManagerTest, RowLockTest1) { RowLockTest1(); }  // NOLINT

void TwoPLTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  RID rid0{0, 0};
  RID rid1{0, 1};

  auto *txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  EXPECT_TRUE(res);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  EXPECT_TRUE(res);

  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 0);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 1);

  res = lock_mgr.UnlockRow(txn, oid, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnRowLockSize(txn, oid, 0, 1);

  try {
    lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  } catch (TransactionAbortException &e) {
    CheckAborted(txn);
    CheckTxnRowLockSize(txn, oid, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnRowLockSize(txn, oid, 0, 0);
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  delete txn;
}

TEST(LockManagerTest, TwoPLTest1) { TwoPLTest1(); }  // NOLINT

}  // namespace bustub

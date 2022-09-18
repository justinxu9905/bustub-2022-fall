/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
  EXPECT_FALSE(table->Find(8, result));
  EXPECT_FALSE(table->Find(4, result));
  EXPECT_FALSE(table->Find(1, result));
}

TEST(ExtendibleHashTableTest, BigTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  const int num_ops = 1000;
  for (int op = 0; op < num_ops; op++) {
    std::string result;
    table->Insert(op, op % 3 ? "a" : (op % 3 == 1 ? "b" : "c"));
    for (int find = 0; find <= op; find++) {
      EXPECT_TRUE(table->Find(find, result));
      EXPECT_EQ(result, find % 3 ? "a" : (find % 3 == 1 ? "b" : "c"));
    }
  }
  for (int op = num_ops / 2 - 1; op >= 0; op--) {
    std::string result;
    table->Remove(op);
    EXPECT_FALSE(table->Find(op, result));
  }
  for (int op = 0; op < num_ops; op++) {
    std::string result;
    if (op < num_ops / 2) {
      EXPECT_FALSE(table->Find(op, result));
    }
    else {
      EXPECT_TRUE(table->Find(op, result));
      EXPECT_EQ(result, op % 3 ? "a" : (op % 3 == 1 ? "b" : "c"));
    }
  }
  for (int op = 0; op < num_ops; op++) {
    std::string result;
    table->Insert(op, "d");
    EXPECT_TRUE(table->Find(op, result));
    EXPECT_EQ(result, "d");
  }
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    //table->Display();

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

}  // namespace bustub

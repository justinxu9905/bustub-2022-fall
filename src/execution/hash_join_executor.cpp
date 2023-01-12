//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple left_tuple;
  RID left_rid;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    HashJoinKey left_key = MakeHashJoinLeftKey(&left_tuple, left_executor_->GetOutputSchema());
    if (hash_table_.find(left_key) == hash_table_.end()) {
      hash_table_[left_key] = std::vector<Tuple>();
      left_done_[left_key] = false;
    }
    hash_table_[left_key].push_back(left_tuple);
  }
}

auto HashJoinExecutor::AntiLeftJoinTuple(Tuple *left_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(idx).GetType()));
  }

  return Tuple{values, &GetOutputSchema()};
}

auto HashJoinExecutor::InnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), idx));
  }

  return Tuple{values, &GetOutputSchema()};
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!queue_.empty()) {
      *tuple = queue_.front();
      *rid = tuple->GetRid();

      queue_.pop();

      return true;
    }

    Tuple right_tuple;
    RID right_rid;
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      break;
    }

    HashJoinKey right_key = MakeHashJoinRightKey(&right_tuple, right_executor_->GetOutputSchema());
    if (!hash_table_[right_key].empty()) {
      for (auto left_tuple : hash_table_[right_key]) {
        queue_.push(InnerJoinTuple(&left_tuple, &right_tuple));
      }
      left_done_[right_key] = true;
    }
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    for (const auto &iter : left_done_) {
      if (!iter.second) {
        if (!hash_table_[iter.first].empty()) {
          Tuple left_tuple = hash_table_[iter.first].back();
          hash_table_[iter.first].pop_back();

          *tuple = AntiLeftJoinTuple(&left_tuple);
          *rid = tuple->GetRid();
          return true;
        }
      }
    }
  }

  return false;
}

}  // namespace bustub

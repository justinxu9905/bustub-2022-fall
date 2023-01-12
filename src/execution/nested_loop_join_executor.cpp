//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  RID left_rid;
  left_ret_ = left_executor_->Next(&left_tuple_, &left_rid);
  left_done_ = false;
}

auto NestedLoopJoinExecutor::AntiLeftJoinTuple(Tuple *left_tuple) -> Tuple {
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

auto NestedLoopJoinExecutor::InnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
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

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID left_rid;
  RID right_rid;

  while (true) {
    if (!left_ret_) {
      return false;
    }

    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->GetJoinType() == JoinType::LEFT && !left_done_) {
        *tuple = AntiLeftJoinTuple(&left_tuple_);
        *rid = tuple->GetRid();

        left_done_ = true;
        return true;
      }

      right_executor_->Init();
      left_ret_ = left_executor_->Next(&left_tuple_, &left_rid);
      left_done_ = false;
      continue;
    }

    auto ret = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                               right_executor_->GetOutputSchema());
    if (!ret.IsNull() && ret.GetAs<bool>()) {
      *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);
      *rid = tuple->GetRid();

      left_done_ = true;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
